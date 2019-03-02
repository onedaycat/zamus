package kinesisstream

import (
	"context"
	"sync"

	errs "github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/errors"
	"golang.org/x/sync/errgroup"
)

type partitionStrategy struct {
	pkPool        sync.Pool
	errorHandlers []EventMessagesErrorHandler
	handlers      []EventMessagesHandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
}

func NewPartitionStrategy() KinesisHandlerStrategy {
	ps := &partitionStrategy{
		eventTypes: make(map[string]struct{}, 20),
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

func (c *partitionStrategy) RegisterHandlers(handlers ...EventMessagesHandler) {
	c.handlers = handlers
}

func (c *partitionStrategy) Process(ctx context.Context, records Records) error {
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

	wg := errgroup.Group{}

	for _, ghs := range partitions {
		ghs := ghs
		if len(ghs) == 0 {
			continue
		}

		wg.Go(func() error {
			return c.handle(ctx, ghs)
		})
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *partitionStrategy) doPreHandlers(ctx context.Context, msgs EventMsgs) (err error) {
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

func (c *partitionStrategy) doPostHandler(ctx context.Context, msgs EventMsgs) (err error) {
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

func (c *partitionStrategy) doHandlers(ctx context.Context, msgs EventMsgs) (err error) {
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

func (c *partitionStrategy) handle(ctx context.Context, msgs EventMsgs) (err error) {
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

func (c *partitionStrategy) recover(ctx context.Context, msgs EventMsgs, err *error) {
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
