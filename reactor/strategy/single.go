package strategy

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/dlq"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/reactor"
)

type singlehandler struct {
	Handler   reactor.EventHandler
	EventMsgs event.Msgs
}

func (s *singlehandler) AddEventMsg(msg *event.Msg) bool {
	s.EventMsgs = append(s.EventMsgs, msg)
	return true
}

func (s *singlehandler) Clear() {
	s.EventMsgs = s.EventMsgs[:0]
}

type singleStrategy struct {
	errorHandlers []reactor.ErrorHandler
	handlers      []*singlehandler
	eventTypes    common.Set
	preHandlers   []reactor.EventHandler
	postHandlers  []reactor.EventHandler
	dlq           dlq.DLQ
}

func NewSingle() *singleStrategy {
	s := &singleStrategy{
		eventTypes: common.NewSet(),
		handlers:   make([]*singlehandler, 0, 1),
	}

	return s
}

func (c *singleStrategy) ErrorHandlers(handlers ...reactor.ErrorHandler) {
	c.errorHandlers = append(c.errorHandlers, handlers...)
}

func (c *singleStrategy) SetDLQ(dlq dlq.DLQ) {
	c.dlq = dlq
}

func (c *singleStrategy) PreHandlers(handlers ...reactor.EventHandler) {
	c.preHandlers = append(c.preHandlers, handlers...)
}

func (c *singleStrategy) PostHandlers(handlers ...reactor.EventHandler) {
	c.postHandlers = append(c.postHandlers, handlers...)
}

func (c *singleStrategy) RegisterHandler(handler reactor.EventHandler, filterEvents []string) {
	c.handlers = append(c.handlers, &singlehandler{
		Handler:   handler,
		EventMsgs: make(event.Msgs, 0, 1),
	})
}

func (c *singleStrategy) Process(ctx context.Context, msgs event.Msgs) errors.Error {
	for i := 0; i < len(c.handlers); i++ {
		c.handlers[i].Clear()
	}

	for i := 0; i < len(c.handlers); i++ {
		c.handlers[i].AddEventMsg(msgs[0])
	}
DLQRetry:

	var err errors.Error
	for i := 0; i < len(c.handlers); i++ {
		if len(c.handlers[i].EventMsgs) == 0 {
			continue
		}

		if err = c.handle(ctx, c.handlers[i].Handler, c.handlers[i].EventMsgs); err != nil {
			break
		}
	}

	if err != nil {
		if c.dlq != nil {
			if ok := c.dlq.Retry(); ok {
				goto DLQRetry
			}

			msgList := &event.MsgList{
				Msgs: []*event.Msg{msgs[0]},
			}

			msgListByte, _ := event.MarshalMsg(msgList)

			return c.dlq.Save(ctx, msgListByte)
		}
		return err
	}

	return nil
}

func (c *singleStrategy) doHandlers(ctx context.Context, handler reactor.EventHandler, msgs event.Msgs) (err errors.Error) {
	defer c.recover(ctx, msgs, &err)
	if err = handler(ctx, msgs); err != nil {
		if c.dlq != nil {
			c.dlq.AddError(err)
		}
		for _, errhandler := range c.errorHandlers {
			errhandler(ctx, msgs, err)
		}
		return err
	}

	return nil
}

func (c *singleStrategy) handle(ctx context.Context, handler reactor.EventHandler, msgs event.Msgs) (err errors.Error) {
	if len(msgs) == 0 {
		return nil
	}

	defer c.recover(ctx, msgs, &err)
	for _, ph := range c.preHandlers {
		if err = ph(ctx, msgs); err != nil {
			if c.dlq != nil {
				c.dlq.AddError(err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}

			return err
		}
	}

	if err = c.doHandlers(ctx, handler, msgs); err != nil {
		return err
	}

	for _, ph := range c.postHandlers {
		if err = ph(ctx, msgs); err != nil {
			if c.dlq != nil {
				c.dlq.AddError(err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}
			return err
		}
	}

	return
}

func (c *singleStrategy) recover(ctx context.Context, msgs event.Msgs, err *errors.Error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(msgs)
			if c.dlq != nil {
				c.dlq.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(msgs)
			if c.dlq != nil {
				c.dlq.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
		}
	}
}
