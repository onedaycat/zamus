package dynamostream

import (
	"sync"
)

type concurrencyManager struct {
	keyChans map[string]chan *Record
	wg       sync.WaitGroup
}

func newConcurrencyManager(nwork int) *concurrencyManager {
	c := &concurrencyManager{
		keyChans: make(map[string]chan *Record),
	}

	c.wg.Add(nwork)

	return c
}

func (c *concurrencyManager) Send(record *Record, getKey KeyHandler, handler EventMessageHandler, onError EventMessageErrorHandler) {
	key := getKey(record)
	keyChan, ok := c.keyChans[key]
	if !ok {
		c.keyChans[key] = make(chan *Record, 1)
		keyChan = c.keyChans[key]
		go func() {
			for {
				rec, more := <-keyChan
				if !more {
					return
				}
				if err := handler(rec.DynamoDB.NewImage.EventMessage); err != nil {
					onError(rec.DynamoDB.NewImage.EventMessage, err)
				}
				// fmt.Println("do", key, *(rec.DynamoDB.Keys["id"].S), rec.DynamoDB.NewImage.EventMessage.EventID)
				c.wg.Done()
			}
		}()
	}

	keyChan <- record
}

func (c *concurrencyManager) Wait() {
	c.wg.Wait()
	c.Close()
}

func (c *concurrencyManager) Close() {
	for _, keyChan := range c.keyChans {
		close(keyChan)
	}
}
