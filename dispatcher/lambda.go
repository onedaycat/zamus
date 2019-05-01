package dispatcher

import (
    "context"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/invoke"
)

type LambdaConfig struct {
    Fn           string
    FilterEvents []string
    Async        bool
    Client       invoke.Invoker

    records    *event.MsgList
    eventTypes map[string]struct{}
    isAll      bool
    ctx        context.Context
}

func (c *LambdaConfig) init() {
    if len(c.FilterEvents) > 0 {
        c.eventTypes = make(map[string]struct{})
        for _, eventType := range c.FilterEvents {
            c.eventTypes[eventType] = struct{}{}
        }
    } else {
        c.isAll = true
    }
    c.records = &event.MsgList{
        Msgs: make(event.Msgs, 0, 100),
    }
}

func (c *LambdaConfig) filter(msg *event.Msg) {
    if c.isAll {
        c.records.Msgs = append(c.records.Msgs, msg)
    } else {
        _, ok := c.eventTypes[msg.EventType]
        if ok {
            c.records.Msgs = append(c.records.Msgs, msg)
        }
    }
}

func (c *LambdaConfig) clear() {
    c.records.Msgs = c.records.Msgs[:0]
}

func (c *LambdaConfig) hasEvents() bool {
    return len(c.records.Msgs) > 0
}

func (c *LambdaConfig) setContext(ctx context.Context) {
    c.ctx = ctx
}

func (c *LambdaConfig) publish() errors.Error {
    if c.Async {
        req := invoke.NewReactorRequest(c.Fn).WithEventList(c.records)
        err := c.Client.InvokeReactorAsync(c.ctx, req)
        if err != nil && appErr.ErrUnableInvokeFunction.GetCode() == err.GetCode() {
            return err
        }
    } else {
        req := invoke.NewReactorRequest(c.Fn).WithEventList(c.records)
        err := c.Client.InvokeReactor(c.ctx, req)
        if err != nil && appErr.ErrUnableInvokeFunction.GetCode() == err.GetCode() {
            return err
        }
    }

    return nil
}
