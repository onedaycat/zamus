package kinesisstream

import (
	"sync"
)

type GroupConcurrency struct {
	wg            sync.WaitGroup
	partitions    map[string]EventMsgs
	errorHandlers []EventMessagesErrorHandler
	handler       EventMessagesHandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
}

func NewGroupConcurrency() *GroupConcurrency {
	return &GroupConcurrency{
		partitions: make(map[string]EventMsgs, 100),
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

func (c *GroupConcurrency) RegisterHandler(handler EventMessagesHandler) {
	c.handler = handler
}

func (c *GroupConcurrency) Process(records Records) {
	var eventType string
	var pk string

	for _, record := range records {
		eventType = record.Kinesis.Data.EventMsg.EventType
		if _, ok := c.eventTypes[eventType]; !ok {
			continue
		}

		pk = record.Kinesis.PartitionKey
		if _, ok := c.partitions[pk]; !ok {
			c.partitions[pk] = make(EventMsgs, 0, 100)
		}

		c.partitions[pk] = append(c.partitions[pk], record.Kinesis.Data.EventMsg)
	}

	c.wg.Add(len(c.partitions))

	for _, ghs := range c.partitions {
		go c.handle(ghs)
	}
}

func (c *GroupConcurrency) handle(msgs EventMsgs) {
	var err error
	for _, ph := range c.preHandlers {
		if err = ph(msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, err)
			}
		}
	}

	if err = c.handler(msgs); err != nil {
		for _, errhandler := range c.errorHandlers {
			errhandler(msgs, err)
		}
	}

	for _, ph := range c.postHandlers {
		if err = ph(msgs); err != nil {
			for _, errhandler := range c.errorHandlers {
				errhandler(msgs, err)
			}
		}
	}

	c.wg.Done()
}

func (c *GroupConcurrency) Wait() {
	c.wg.Wait()
}
