package kinesisstream

import (
	"fmt"
	"sync"
)

type groupConcurrencyManager struct {
	recordKeys map[string]EventMessages
	wg         sync.WaitGroup
}

func newGroupConcurrencyManager(nwork int) *groupConcurrencyManager {
	return &groupConcurrencyManager{
		recordKeys: make(map[string]EventMessages),
	}
}

func (c *groupConcurrencyManager) Send(records Records, handler EventMessagesHandler, onError EventMessagesErrorHandler) {
	for _, record := range records {
		key := record.Kinesis.PartitionKey
		_, ok := c.recordKeys[key]
		if !ok {
			c.recordKeys[key] = make(EventMessages, 0, 100)
		}
		c.recordKeys[key] = append(c.recordKeys[key], record.Kinesis.Data.EventMessage)
	}

	c.wg.Add(len(c.recordKeys))

	for key, recordKey := range c.recordKeys {
		go func(msgs EventMessages, k string) {
			fmt.Println("do", k)
			if err := handler(msgs); err != nil {
				onError(msgs, err)
			}
			c.wg.Done()
		}(recordKey, key)
	}
}

func (c *groupConcurrencyManager) Wait() {
	c.wg.Wait()
}
