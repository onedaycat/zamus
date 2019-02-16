package dynamostream

import (
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

func (c *groupConcurrencyManager) Send(records Records, getKey KeyHandler, handler EventMessagesHandler, onError EventMessagesErrorHandler) {
	for _, record := range records {
		if record.EventName != EventInsert {
			continue
		}

		key := getKey(record)
		_, ok := c.recordKeys[key]
		if !ok {
			c.recordKeys[key] = make(EventMessages, 0, 100)
		}
		c.recordKeys[key] = append(c.recordKeys[key], record.DynamoDB.NewImage.EventMessage)
	}

	c.wg.Add(len(c.recordKeys))

	for key, recordKey := range c.recordKeys {
		go func(msgs EventMessages, k string) {
			// fmt.Println("do", k)
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
