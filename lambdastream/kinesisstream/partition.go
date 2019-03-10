package kinesisstream

import (
	"context"
	"strconv"
	"sync"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/dql"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/tracer"
)

type partitionStrategy struct {
	pkPool        sync.Pool
	errorHandlers []EventMessagesErrorHandler
	handlers      []*handlerInfo
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
	dql           dql.DQL
}

func NewPartitionStrategy() KinesisHandlerStrategy {
	ps := &partitionStrategy{
		eventTypes: make(map[string]struct{}, 20),
		handlers:   make([]*handlerInfo, 0, 10),
	}

	ps.pkPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]EventMsgs, 100)
		},
	}

	return ps
}

func (c *partitionStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
}

func (c *partitionStrategy) SetDQL(dql dql.DQL) {
	c.dql = dql
}

func (c *partitionStrategy) FilterEvents(eventTypes ...string) {
	for _, eventType := range eventTypes {
		c.eventTypes[eventType] = struct{}{}
	}
}

func (c *partitionStrategy) PreHandlers(handlers ...EventMessagesHandler) {
	c.preHandlers = handlers
}

func (c *partitionStrategy) PostHandlers(handlers ...EventMessagesHandler) {
	c.postHandlers = handlers
}

func (c *partitionStrategy) RegisterHandler(handler EventMessagesHandler, filterEvents FilterEvents) {
	if filterEvents == nil {
		c.handlers = append(c.handlers, &handlerInfo{
			Handler:      handler,
			FilterEvents: common.NewSet(),
		})
	} else {
		c.handlers = append(c.handlers, &handlerInfo{
			Handler:      handler,
			FilterEvents: common.NewSetFromList(filterEvents()),
		})
	}
}

func (c *partitionStrategy) Process(ctx context.Context, records Records) errors.Error {
	var eventType string
	var pk string
	partitions := c.pkPool.Get().(map[string]EventMsgs)
	defer func() {
		for key := range partitions {
			partitions[key] = make(EventMsgs, 0, 100)
		}
		for key := range partitions {
			delete(partitions, key)
		}
		c.pkPool.Put(partitions)

	}()

	if len(c.eventTypes) > 0 {
		for _, record := range records {
			// fmt.Println("###", record.Kinesis.Data.EventMsg.EventID, record.Kinesis.Data.EventMsg.Seq)
			eventType = record.Kinesis.Data.EventMsg.EventType
			if _, ok := c.eventTypes[eventType]; !ok {
				continue
			}

			pk = record.Kinesis.PartitionKey
			if _, ok := partitions[pk]; !ok {
				partitions[pk] = make(EventMsgs, 0, 100)
			}

			partitions[pk] = append(partitions[pk], record.Kinesis.Data.EventMsg)
		}
	} else {
		for _, record := range records {
			pk = record.Kinesis.PartitionKey
			if _, ok := partitions[pk]; !ok {
				partitions[pk] = make(EventMsgs, 0, 100)
			}

			partitions[pk] = append(partitions[pk], record.Kinesis.Data.EventMsg)
		}
	}

DQLRetry:

	wg := errgroup.Group{}

	for _, ghs := range partitions {
		ghs := ghs
		if len(ghs) == 0 {
			continue
		}

		wg.Go(func() errors.Error {
			return c.handle(ctx, ghs)
		})
	}

	if err := wg.Wait(); err != nil {
		if c.dql != nil {
			if ok := c.dql.Retry(); ok {
				goto DQLRetry
			}

			msgs := make(EventMsgs, len(records))
			for i, record := range records {
				msgs[i] = record.Kinesis.Data.EventMsg
			}

			return c.dql.Save(ctx, msgs)
		}

		return err
	}

	return nil
}

func (c *partitionStrategy) doPreHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
	defer c.recover(ctx, msgs, &err)
	for _, ph := range c.preHandlers {
		if err = ph(ctx, msgs); err != nil {
			if c.dql != nil {
				c.dql.AddError(err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}

			return err
		}
	}

	return
}

func (c *partitionStrategy) doPostHandler(ctx context.Context, msgs EventMsgs) (err errors.Error) {
	defer c.recover(ctx, msgs, &err)
	for _, ph := range c.postHandlers {
		if err = ph(ctx, msgs); err != nil {
			if c.dql != nil {
				c.dql.AddError(err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}
		}

		return err
	}

	return
}

func (c *partitionStrategy) filterEvents(info *handlerInfo, msgs EventMsgs) EventMsgs {
	if info.FilterEvents.IsEmpty() {
		return msgs
	}

	var ok bool
	fillter := make(EventMsgs, 0, len(msgs))
	for _, msg := range msgs {
		if ok = info.FilterEvents.Has(msg.EventType); ok {
			fillter = append(fillter, msg)
		}
	}

	return fillter
}

func (c *partitionStrategy) doHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
	wg := errgroup.Group{}
	for i, handlerinfo := range c.handlers {
		handlerinfo := handlerinfo
		wg.Go(func() (aerr errors.Error) {
			hctx, seg := tracer.BeginSubsegment(ctx, "handler_"+strconv.Itoa(i))
			defer tracer.Close(seg)
			defer c.recover(hctx, msgs, &aerr)
			newmsgs := c.filterEvents(handlerinfo, msgs)
			if len(newmsgs) == 0 {
				return nil
			}

			if aerr = handlerinfo.Handler(hctx, newmsgs); aerr != nil {
				tracer.AddError(seg, aerr)
				if c.dql != nil {
					c.dql.AddError(aerr)
				}
				for _, errhandler := range c.errorHandlers {
					errhandler(hctx, newmsgs, aerr)
				}
				return aerr
			}

			return
		})
	}

	return wg.Wait()
}

func (c *partitionStrategy) handle(ctx context.Context, msgs EventMsgs) (err errors.Error) {
	if len(msgs) == 0 {
		return nil
	}

	if err = c.doPreHandlers(ctx, msgs); err != nil {
		return err
	}

	if err = c.doHandlers(ctx, msgs); err != nil {
		return err
	}

	if err = c.doPostHandler(ctx, msgs); err != nil {
		return err
	}

	return
}

func (c *partitionStrategy) recover(ctx context.Context, msgs EventMsgs, err *errors.Error) {
	if r := recover(); r != nil {
		seg := tracer.GetSegment(ctx)
		defer tracer.Close(seg)
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrInternalError.WithPanic().WithCause(cause).WithCallerSkip(6)
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(seg, *err)
		default:
			*err = appErr.ErrInternalError.WithPanic().WithInput(cause).WithCallerSkip(6)
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(seg, *err)
		}
	}
}
