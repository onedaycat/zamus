package dynamostream

import (
	"sync"
)

type GroupConcurrency struct {
	wg            sync.WaitGroup
	errorHandlers []EventMessagesErrorHandler
	handler       EventMessagesHandler
	eventTypes    map[string]struct{}
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
}

func NewGroupConcurrency() *GroupConcurrency {
	return &GroupConcurrency{
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
	partitions := make(map[string]EventMsgs, 100)

	for _, record := range records {
		eventType = record.DynamoDB.NewImage.EventMsg.EventType
		if _, ok := c.eventTypes[eventType]; !ok {
			continue
		}

		pk = record.DynamoDB.NewImage.EventMsg.AggregateID
		if _, ok := partitions[pk]; !ok {
			partitions[pk] = make(EventMsgs, 0, 100)
		}

		partitions[pk] = append(partitions[pk], record.DynamoDB.NewImage.EventMsg)
	}

	c.wg.Add(len(partitions))

	for _, ghs := range partitions {
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
