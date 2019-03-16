package kinesisstream

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/dql"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/tracer"
)

type simplehandler struct {
	Handler      EventMessagesHandler
	FilterEvents common.SetList
	EventMsgs    EventMsgs
}

func (s *simplehandler) AddEventMsg(msg *EventMsg) bool {
	if s.FilterEvents == nil {
		s.EventMsgs = append(s.EventMsgs, msg)
		return true
	}

	if s.FilterEvents.Has(msg.EventType) {
		s.EventMsgs = append(s.EventMsgs, msg)
		return true
	}

	return false
}

func (s *simplehandler) Clear() {
	s.EventMsgs = s.EventMsgs[:0]
}

type simpleStrategy struct {
	errorHandlers []EventMessagesErrorHandler
	handlers      []*simplehandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
	dql           dql.DQL
}

func NewSimpleStrategy() KinesisHandlerStrategy {
	s := &simpleStrategy{
		eventTypes: make(map[string]struct{}, 20),
		handlers:   make([]*simplehandler, 0, 10),
	}

	return s
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
		c.handlers = append(c.handlers, &simplehandler{
			Handler:      handler,
			FilterEvents: nil,
			EventMsgs:    make(EventMsgs, 0, 100),
		})
	} else {
		c.handlers = append(c.handlers, &simplehandler{
			Handler:      handler,
			FilterEvents: common.NewSetListFromList(filterEvents()),
			EventMsgs:    make(EventMsgs, 0, 100),
		})
	}
}

func (c *simpleStrategy) Process(ctx context.Context, records Records) errors.Error {

	for i := 0; i < len(c.handlers); i++ {
		c.handlers[i].Clear()
	}

	if len(c.eventTypes) > 0 {
		var eventType string
		for _, record := range records {
			eventType = record.Kinesis.Data.EventMsg.EventType
			if _, ok := c.eventTypes[eventType]; !ok {
				continue
			}

			for i := 0; i < len(c.handlers); i++ {
				c.handlers[i].AddEventMsg(record.Kinesis.Data.EventMsg)
			}
		}
	} else {
		for _, record := range records {
			for i := 0; i < len(c.handlers); i++ {
				c.handlers[i].AddEventMsg(record.Kinesis.Data.EventMsg)
			}
		}
	}
DQLRetry:

	var err errors.Error
	var eventType string
	for i := 0; i < len(c.handlers); i++ {
		if len(c.handlers[i].EventMsgs) == 0 {
			continue
		}

		if err = c.handle(ctx, c.handlers[i].Handler, c.handlers[i].EventMsgs); err != nil {
			break
		}
	}

	if err != nil {
		if c.dql != nil {
			if ok := c.dql.Retry(); ok {
				goto DQLRetry
			}

			msgs := make(EventMsgs, len(records))
			for i, record := range records {
				eventType = record.Kinesis.Data.EventMsg.EventType
				if _, ok := c.eventTypes[eventType]; !ok {
					continue
				}
				msgs[i] = record.Kinesis.Data.EventMsg
			}

			msgList := eventstore.EventMsgList{
				EventMsgs: msgs,
			}
			msgListByte, _ := msgList.Marshal()

			return c.dql.Save(ctx, msgListByte)
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

func (c *simpleStrategy) doHandlers(ctx context.Context, handler EventMessagesHandler, msgs EventMsgs) (err errors.Error) {
	defer c.recover(ctx, msgs, &err)
	if err = handler(ctx, msgs); err != nil {
		tracer.AddError(ctx, err)
		if c.dql != nil {
			c.dql.AddError(err)
		}
		for _, errhandler := range c.errorHandlers {
			errhandler(ctx, msgs, err)
		}
		return err
	}

	return nil
}

func (c *simpleStrategy) handle(ctx context.Context, handler EventMessagesHandler, msgs EventMsgs) (err errors.Error) {
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

	if err = c.doHandlers(ctx, handler, msgs); err != nil {
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
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(msgs)
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(ctx, *err)
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(msgs)
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(ctx, *err)
		}
	}
}
