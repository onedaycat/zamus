package kinesisstream

import (
	"context"
	"strconv"

	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/tracer"
)

type simpleStrategy struct {
	errorHandlers []EventMessagesErrorHandler
	handlers      []*handlerInfo
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
	dql           dql.DQL
}

func NewSimpleStrategy() KinesisHandlerStrategy {
	return &simpleStrategy{
		eventTypes: make(map[string]struct{}, 20),
		handlers:   make([]*handlerInfo, 0, 10),
	}
}

func (c *simpleStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
}

func (c *simpleStrategy) SetDQL(dql dql.DQL) {
	c.dql = dql
}

func (c *simpleStrategy) FilterEvents(eventTypes ...string) {
	for _, eventType := range eventTypes {
		c.eventTypes[eventType] = struct{}{}
	}
}

func (c *simpleStrategy) PreHandlers(handlers ...EventMessagesHandler) {
	c.preHandlers = handlers
}

func (c *simpleStrategy) PostHandlers(handlers ...EventMessagesHandler) {
	c.postHandlers = handlers
}

func (c *simpleStrategy) RegisterHandler(handler EventMessagesHandler, filterEvents FilterEvents) {
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

func (c *simpleStrategy) Process(ctx context.Context, records Records) errors.Error {
	var eventType string
	msgs := make(EventMsgs, 0, 100)

	if len(c.eventTypes) > 0 {
		for _, record := range records {
			eventType = record.Kinesis.Data.EventMsg.EventType
			if _, ok := c.eventTypes[eventType]; !ok {
				continue
			}

			msgs = append(msgs, record.Kinesis.Data.EventMsg)
		}
	} else {
		for _, record := range records {
			msgs = append(msgs, record.Kinesis.Data.EventMsg)
		}
	}

DQLRetry:

	if err := c.handle(ctx, msgs); err != nil {
		if c.dql != nil {
			if ok := c.dql.Retry(); ok {
				goto DQLRetry
			}

			return c.dql.Save(ctx, msgs)
		}
		return err
	}

	return nil
}

func (c *simpleStrategy) filterEvents(info *handlerInfo, msgs EventMsgs) EventMsgs {
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

func (c *simpleStrategy) doHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
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

			if aerr = handlerinfo.Handler(hctx, c.filterEvents(handlerinfo, newmsgs)); aerr != nil {
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

func (c *simpleStrategy) handle(ctx context.Context, msgs EventMsgs) (err errors.Error) {
	if len(msgs) == 0 {
		return nil
	}

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

	if err = c.doHandlers(ctx, msgs); err != nil {
		return err
	}

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

func (c *simpleStrategy) recover(ctx context.Context, msgs EventMsgs, err *errors.Error) {
	if r := recover(); r != nil {
		seg := tracer.GetSegment(ctx)
		defer tracer.Close(seg)
		switch cause := r.(type) {
		case error:
			*err = errors.ErrPanic.WithCause(cause).WithCallerSkip(6)
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(seg, *err)
		default:
			*err = errors.ErrPanic.WithInput(cause).WithCallerSkip(6)
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
