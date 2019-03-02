package kinesisstream

import (
	"context"

	errs "github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/errors"
	"golang.org/x/sync/errgroup"
)

type shardStrategy struct {
	nShard        int
	errorHandlers []EventMessagesErrorHandler
	handlers      []EventMessagesHandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
}

func NewShardStrategy(shard int) KinesisHandlerStrategy {
	return &shardStrategy{
		nShard:     shard,
		eventTypes: make(map[string]struct{}, 20),
	}
}

func (c *shardStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
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

func (c *shardStrategy) RegisterHandlers(handlers ...EventMessagesHandler) {
	c.handlers = handlers
}

func (c *shardStrategy) Process(ctx context.Context, records Records) error {
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

	wg := errgroup.Group{}

	for _, shard := range shards {
		shard := shard
		if len(shard) == 0 {
			continue
		}

		wg.Go(func() (err error) {
			defer c.recover(ctx, shard, &err)
			return c.handle(ctx, shard)
		})
	}

	return wg.Wait()
}

func (c *shardStrategy) doPreHandlers(ctx context.Context, msgs EventMsgs) (err error) {
	defer c.recover(ctx, msgs, &err)
	for _, ph := range c.preHandlers {
		if err = ph(ctx, msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}

			return err
		}
	}

	return
}

func (c *shardStrategy) doPostHandler(ctx context.Context, msgs EventMsgs) (err error) {
	defer c.recover(ctx, msgs, &err)
	for _, ph := range c.postHandlers {
		if err = ph(ctx, msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}
		}

		return err
	}

	return
}

func (c *shardStrategy) doHandlers(ctx context.Context, msgs EventMsgs) (err error) {
	wg := errgroup.Group{}
	for _, handler := range c.handlers {
		handler := handler
		wg.Go(func() (aerr error) {
			defer c.recover(ctx, msgs, &aerr)
			if err := handler(ctx, msgs); err != nil {
				for _, errhandler := range c.errorHandlers {
					errhandler(ctx, msgs, err)
				}
				return err
			}

			return
		})
	}
	if err = wg.Wait(); err != nil {
		return err
	}

	return
}

func (c *shardStrategy) handle(ctx context.Context, msgs EventMsgs) (err error) {
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

func (c *shardStrategy) recover(ctx context.Context, msgs EventMsgs, err *error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			appErr := errors.ErrPanic.WithCause(cause).WithCallerSkip(6)
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, appErr)
			}
			*err = appErr
		case string:
			appErr := errors.ErrPanic.WithCause(errs.New(cause)).WithCallerSkip(6)
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, appErr)
			}
			*err = appErr
		default:
			panic(cause)
		}
	}
}
