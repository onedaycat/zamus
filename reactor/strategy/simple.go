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

type simplehandler struct {
    Handler      reactor.EventHandler
    FilterEvents common.Set
    EventMsgs    event.Msgs
}

func (s *simplehandler) AddEventMsg(msg *event.Msg) bool {
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
    errorHandlers []reactor.ErrorHandler
    handlers      []*simplehandler
    eventTypes    common.Set
    preHandlers   []reactor.EventHandler
    postHandlers  []reactor.EventHandler
    dlq           dlq.DLQ
}

func NewSimple() *simpleStrategy {
    s := &simpleStrategy{
        eventTypes: common.NewSet(),
        handlers:   make([]*simplehandler, 0, 10),
    }

    return s
}

func (c *simpleStrategy) ErrorHandlers(handlers ...reactor.ErrorHandler) {
    c.errorHandlers = append(c.errorHandlers, handlers...)
}

func (c *simpleStrategy) SetDLQ(dlq dlq.DLQ) {
    c.dlq = dlq
}

func (c *simpleStrategy) PreHandlers(handlers ...reactor.EventHandler) {
    c.preHandlers = append(c.preHandlers, handlers...)
}

func (c *simpleStrategy) PostHandlers(handlers ...reactor.EventHandler) {
    c.postHandlers = append(c.postHandlers, handlers...)
}

func (c *simpleStrategy) RegisterHandler(handler reactor.EventHandler, filterEvents []string) {
    if filterEvents == nil {
        c.handlers = append(c.handlers, &simplehandler{
            Handler:      handler,
            FilterEvents: nil,
            EventMsgs:    make(event.Msgs, 0, 100),
        })
    } else {
        c.handlers = append(c.handlers, &simplehandler{
            Handler:      handler,
            FilterEvents: common.NewSetFromList(filterEvents),
            EventMsgs:    make(event.Msgs, 0, 100),
        })
        c.eventTypes.SetMany(filterEvents)
    }
}

func (c *simpleStrategy) Process(ctx context.Context, msgs event.Msgs) errors.Error {

    for i := 0; i < len(c.handlers); i++ {
        c.handlers[i].Clear()
    }

    if len(c.eventTypes) > 0 {
        for _, msg := range msgs {
            if !c.eventTypes.Has(msg.EventType) {
                continue
            }

            for i := 0; i < len(c.handlers); i++ {
                c.handlers[i].AddEventMsg(msg)
            }
        }
    } else {
        for _, msg := range msgs {
            for i := 0; i < len(c.handlers); i++ {
                c.handlers[i].AddEventMsg(msg)
            }
        }
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

            msgs := make(event.Msgs, 0, len(msgs))
            if len(c.eventTypes) > 0 {
                for _, msg := range msgs {
                    if !c.eventTypes.Has(msg.EventType) {
                        continue
                    }
                    msgs = append(msgs, msg)
                }
            } else {
                for _, msg := range msgs {
                    msgs = append(msgs, msg)
                }
            }

            msgList := &event.MsgList{
                Msgs: msgs,
            }

            msgListByte, _ := event.MarshalMsg(msgList)

            return c.dlq.Save(ctx, msgListByte)
        }
        return err
    }

    return nil
}

func (c *simpleStrategy) doHandlers(ctx context.Context, handler reactor.EventHandler, msgs event.Msgs) (err errors.Error) {
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

func (c *simpleStrategy) handle(ctx context.Context, handler reactor.EventHandler, msgs event.Msgs) (err errors.Error) {
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

func (c *simpleStrategy) recover(ctx context.Context, msgs event.Msgs, err *errors.Error) {
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
