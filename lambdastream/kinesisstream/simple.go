package kinesisstream

import (
	errs "github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/errors"
	"golang.org/x/sync/errgroup"
)

type simpleStrategy struct {
	errorHandlers []EventMessagesErrorHandler
	handlers      []EventMessagesHandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
}

func NewSimpleStrategy() KinesisHandlerStrategy {
	return &simpleStrategy{
		eventTypes: make(map[string]struct{}, 20),
	}
}

func (c *simpleStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
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

func (c *simpleStrategy) RegisterHandlers(handlers ...EventMessagesHandler) {
	c.handlers = handlers
}

func (c *simpleStrategy) Process(records Records) error {
	var eventType string
	msgs := make(EventMsgs, 0, 100)

	for _, record := range records {
		eventType = record.Kinesis.Data.EventMsg.EventType
		if _, ok := c.eventTypes[eventType]; !ok {
			continue
		}

		msgs = append(msgs, record.Kinesis.Data.EventMsg)
	}

	return c.handle(msgs)
}

func (c *simpleStrategy) handle(msgs EventMsgs) (err error) {
	defer c.recover(msgs, &err)
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

func (c *simpleStrategy) recover(msgs EventMsgs, err *error) {
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
