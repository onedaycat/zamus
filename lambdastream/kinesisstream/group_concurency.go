package kinesisstream

import (
	"fmt"
	"sync"
)

type grouphandler struct {
	handler    EventMessagesHandler
	recordKeys EventMsgs
}

type GroupConcurrency struct {
	wg            sync.WaitGroup
	handlers      map[string]*grouphandler
	errorHandlers []EventMessageErrorHandler
}

func NewGroupConcurrency() *GroupConcurrency {
	return &GroupConcurrency{
		handlers: make(map[string]*grouphandler, 20),
	}
}

func (c *GroupConcurrency) RegisterEvent(eventType string, handler EventMessagesHandler) {
	c.handlers[eventType] = &grouphandler{
		handler:    handler,
		recordKeys: make(EventMsgs, 0, 100),
	}
}

func (c *GroupConcurrency) ErrorHandler(handlers ...EventMessageErrorHandler) {
	c.errorHandlers = handlers
}

func (c *GroupConcurrency) Process(records Records) {
	for _, record := range records {
		gh, ok := c.handlers[record.Kinesis.Data.EventMsg.EventType]
		if !ok {
			continue
		}
		gh.recordKeys = append(gh.recordKeys, record.Kinesis.Data.EventMsg)
	}

	c.wg.Add(len(c.handlers))

	for key, gh := range c.handlers {
		go func(h *grouphandler, k string) {
			fmt.Println("do", k)
			if msg, err := h.handler(h.recordKeys); err != nil {
				for _, errhandler := range c.errorHandlers {
					errhandler(msg, err)
				}
			}
			c.wg.Done()
		}(gh, key)
	}
}

func (c *GroupConcurrency) Wait() {
	c.wg.Wait()
}
