package dispatcher

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/errgroup"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/invoke"
)

type SagaInputFn func(msg *event.Msg) interface{}

type SagaConfig struct {
    Fn          string
    FilterEvent string
    Input       SagaInputFn
    Client      invoke.Invoker

    records map[string]event.Msgs
    ctx     context.Context
    wgSaga  *errgroup.Group
}

func (c *SagaConfig) init() {
    c.records = make(map[string]event.Msgs)
    c.wgSaga = &errgroup.Group{}
}

func (c *SagaConfig) filter(msg *event.Msg) {
    if c.FilterEvent == msg.EventType {
        if _, ok := c.records[msg.AggID]; !ok {
            c.records[msg.AggID] = make(event.Msgs, 0, 100)
        }
        c.records[msg.AggID] = append(c.records[msg.AggID], msg)
    }
}

func (c *SagaConfig) clear() {
    c.records = make(map[string]event.Msgs)
}

func (c *SagaConfig) hasEvents() bool {
    return len(c.records) > 0
}

func (c *SagaConfig) setContext(ctx context.Context) {
    c.ctx = ctx
}

func (c *SagaConfig) publish() errors.Error {
    for _, msgs := range c.records {
        msgs := msgs
        c.wgSaga.Go(func() errors.Error {
            for _, msg := range msgs {
                req := invoke.NewSagaRequest(c.Fn).WithEventMsg(msg)
                _ = c.Client.InvokeSaga(c.ctx, req, nil)
            }

            return nil
        })
    }

    return c.wgSaga.Wait()
}
