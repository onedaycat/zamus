package kinesisstream

import (
	"context"
	"strconv"

	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/tracer"

	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/zamus/errors"
)

type shardStrategy struct {
	nShard        int
	errorHandlers []EventMessagesErrorHandler
	handlers      []*handlerInfo
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
	dql           dql.DQL
}

func NewShardStrategy(shard int) KinesisHandlerStrategy {
	return &shardStrategy{
		nShard:     shard,
		eventTypes: make(map[string]struct{}, 20),
		handlers:   make([]*handlerInfo, 0, 10),
	}
}

func (c *shardStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
}

func (c *shardStrategy) SetDQL(dql dql.DQL) {
	c.dql = dql
}

func (c *shardStrategy) FilterEvents(eventTypes ...string) {
	for _, eventType := range eventTypes {
		c.eventTypes[eventType] = struct{}{}
	}
}

func (c *shardStrategy) PreHandlers(handlers ...EventMessagesHandler) {
	c.preHandlers = handlers
}

func (c *shardStrategy) PostHandlers(handlers ...EventMessagesHandler) {
	c.postHandlers = handlers
}

func (c *shardStrategy) RegisterHandler(handler EventMessagesHandler, filterEvents FilterEvents) {
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

func (c *shardStrategy) Process(ctx context.Context, records Records) errors.Error {
	var eventType string
	var pk string
	var shardPos int
	var pos int
	var ok bool
	shards := make([]EventMsgs, c.nShard)
	pkPos := make(map[string]int, 100)

	for i := 0; i < c.nShard; i++ {
		shards[i] = make(EventMsgs, 0, 100)
	}

	if len(c.eventTypes) > 0 {
		for i, record := range records {
			eventType = record.Kinesis.Data.EventMsg.EventType
			if _, ok := c.eventTypes[eventType]; !ok {
				continue
			}

			pk = record.Kinesis.PartitionKey
			shardPos = i % c.nShard
			pos, ok = pkPos[pk]
			if !ok {
				pkPos[pk] = shardPos
				pos = shardPos
			}

			shards[pos] = append(shards[pos], record.Kinesis.Data.EventMsg)
		}
	} else {
		for i, record := range records {
			pk = record.Kinesis.PartitionKey
			shardPos = i % c.nShard
			pos, ok = pkPos[pk]
			if !ok {
				pkPos[pk] = shardPos
				pos = shardPos
			}

			shards[pos] = append(shards[pos], record.Kinesis.Data.EventMsg)
		}
	}

DQLRetry:
	wg := errgroup.Group{}

	for _, shard := range shards {
		shard := shard
		if len(shard) == 0 {
			continue
		}

		wg.Go(func() (err errors.Error) {
			return c.handle(ctx, shard)
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

func (c *shardStrategy) doPreHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
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

func (c *shardStrategy) doPostHandler(ctx context.Context, msgs EventMsgs) (err errors.Error) {
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

func (c *shardStrategy) filterEvents(info *handlerInfo, msgs EventMsgs) EventMsgs {
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

func (c *shardStrategy) doHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
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

func (c *shardStrategy) handle(ctx context.Context, msgs EventMsgs) (err errors.Error) {
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

func (c *shardStrategy) recover(ctx context.Context, msgs EventMsgs, err *errors.Error) {
	if r := recover(); r != nil {
		seg := tracer.GetSegment(ctx)
		defer tracer.Close(seg)
		switch cause := r.(type) {
		case error:
			*err = errors.ErrPanic.WithCause(cause).WithCallerSkip(6).WithPanic()
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(seg, *err)
		default:
			*err = errors.ErrPanic.WithInput(cause).WithCallerSkip(6).WithPanic()
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
