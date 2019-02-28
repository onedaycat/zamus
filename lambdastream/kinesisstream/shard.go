package kinesisstream

import (
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

func (c *shardStrategy) Process(records Records) error {
	var eventType string
	var pk string
	var shardPos int
	var pos int
	var ok bool
	shards := make([]EventMsgs, c.nShard)
	pkPos := make(map[string]int, 100)

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
		if shards[pos] == nil {
			shards[pos] = make(EventMsgs, 0, 100)
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
			defer c.recover(shard, &err)
			return c.handle(shard)
		})
	}

	return wg.Wait()
}

func (c *shardStrategy) handle(msgs EventMsgs) (err error) {
	for _, ph := range c.preHandlers {
		if err = ph(msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, err)
			}

			return err
		}
	}

	wg := errgroup.Group{}
	for _, handler := range c.handlers {
		handler := handler
		wg.Go(func() (aerr error) {
			defer c.recover(msgs, &aerr)
			if err := handler(msgs); err != nil {
				for _, errhandler := range c.errorHandlers {
					errhandler(msgs, err)
				}
				return err
			}

			return
		})
	}
	if err = wg.Wait(); err != nil {
		return err
	}

	for _, ph := range c.postHandlers {
		if err = ph(msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, err)
			}
		}

		return err
	}

	return
}

func (c *shardStrategy) recover(msgs EventMsgs, err *error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			appErr := errors.ErrPanic.WithCause(cause).WithCallerSkip(6)
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, appErr)
			}
			*err = appErr
		case string:
			appErr := errors.ErrPanic.WithCause(errs.New(cause)).WithCallerSkip(6)
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, appErr)
			}
			*err = appErr
		default:
			panic(cause)
		}
	}
}
