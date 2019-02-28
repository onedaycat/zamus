package kinesisstream

import (
	"sync"

	errs "github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/errors"
	"golang.org/x/sync/errgroup"
)

type GroupConcurrency struct {
	wg            sync.WaitGroup
	errorHandlers []EventMessagesErrorHandler
	handlers      []EventMessagesHandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
}

func NewGroupConcurrency() *GroupConcurrency {
	return &GroupConcurrency{
		eventTypes: make(map[string]struct{}, 20),
	}
}

func (c *GroupConcurrency) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
}

func (c *GroupConcurrency) FilterEvents(eventTypes ...string) {
	for _, eventType := range eventTypes {
		c.eventTypes[eventType] = struct{}{}
	}
}

func (c *GroupConcurrency) PreHandlers(handlers ...EventMessagesHandler) {
	c.preHandlers = handlers
}

func (c *GroupConcurrency) PostHandlers(handlers ...EventMessagesHandler) {
	c.postHandlers = handlers
}

func (c *GroupConcurrency) RegisterHandlers(handlers ...EventMessagesHandler) {
	c.handlers = handlers
}

func (c *GroupConcurrency) Process(records Records) error {
	var eventType string
	var pk string
	partitions := make(map[string]EventMsgs, 100)

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
		wg.Go(func() error {
			return c.handle(ghs)
		})
	}

	return wg.Wait()
}

func (c *GroupConcurrency) doPreHandlers(msgs EventMsgs) (err error) {
	defer c.recover(msgs, &err)
	for _, ph := range c.preHandlers {
		if err = ph(msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, err)
			}

			return err
		}
	}

	return
}

func (c *GroupConcurrency) doPostHandler(msgs EventMsgs) (err error) {
	defer c.recover(msgs, &err)
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

func (c *GroupConcurrency) doHandlers(msgs EventMsgs) (err error) {
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

	return
}

func (c *GroupConcurrency) handle(msgs EventMsgs) (err error) {
	if err = c.doPreHandlers(msgs); err != nil {
		return err
	}

	if err = c.doHandlers(msgs); err != nil {
		return err
	}

	if err = c.doPostHandler(msgs); err != nil {
		return err
	}

	return
}

func (c *GroupConcurrency) recover(msgs EventMsgs, err *error) {
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
