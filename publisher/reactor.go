package publisher

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/invoke"
)

type ReactorConfig struct {
	Fn           string
	FilterEvents []string
	records      *event.MsgList
	eventTypes   map[string]struct{}
	isAll        bool
	client       invoke.Invoker
	ctx          context.Context
}

func (c *ReactorConfig) init() {
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

func (c *ReactorConfig) filter(msg *event.Msg) {
	if c.isAll {
		c.records.Msgs = append(c.records.Msgs, msg)
	} else {
		_, ok := c.eventTypes[msg.EventType]
		if ok {
			c.records.Msgs = append(c.records.Msgs, msg)
		}
	}
}

func (c *ReactorConfig) clear() {
	c.records.Msgs = c.records.Msgs[:0]
}

func (c *ReactorConfig) hasEvents() bool {
	return len(c.records.Msgs) > 0 || c.isAll
}

func (c *ReactorConfig) setContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *ReactorConfig) publish() errors.Error {
	req := invoke.NewReactorRequest(c.Fn).WithEventList(c.records)
	_ = c.client.InvokeReactor(c.ctx, req)

	return nil
}
