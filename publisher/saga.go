package publisher

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/invoke"
)

type SagaConfig struct {
	Fn           string
	FilterEvents []string
	records      map[string]event.Msgs
	eventTypes   map[string]struct{}
	isAll        bool
	client       invoke.Invoker
	ctx          context.Context
	wgSaga       *errgroup.Group
}

func (c *SagaConfig) init() {
	if len(c.FilterEvents) > 0 {
		c.eventTypes = make(map[string]struct{})
		for _, eventType := range c.FilterEvents {
			c.eventTypes[eventType] = struct{}{}
		}
	} else {
		c.isAll = true
	}
	c.records = make(map[string]event.Msgs)
	c.wgSaga = &errgroup.Group{}
}

func (c *SagaConfig) filter(msg *event.Msg) {
	if c.isAll {
		if _, ok := c.records[msg.AggID]; !ok {
			c.records[msg.AggID] = make(event.Msgs, 0, 100)
		}
		c.records[msg.AggID] = append(c.records[msg.AggID], msg)
	} else {
		_, ok := c.eventTypes[msg.EventType]
		if ok {
			if _, ok := c.records[msg.AggID]; !ok {
				c.records[msg.AggID] = make(event.Msgs, 0, 100)
			}

			c.records[msg.AggID] = append(c.records[msg.AggID], msg)
		}
	}
}

func (c *SagaConfig) clear() {
	c.records = make(map[string]event.Msgs)
}

func (c *SagaConfig) hasEvents() bool {
	return len(c.records) > 0 || c.isAll
}

func (c *SagaConfig) setContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *SagaConfig) publish() errors.Error {
	for _, msgs := range c.records {
		msgs := msgs
		c.wgSaga.Go(func() errors.Error {
			for _, msg := range msgs {
				req := invoke.NewSagaRequest(c.Fn).WithInput(msg)
				_ = c.client.InvokeSaga(c.ctx, req, nil)
			}

			return nil
		})
	}

	return c.wgSaga.Wait()
}
