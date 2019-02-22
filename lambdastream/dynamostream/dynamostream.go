package dynamostream

import (
	"context"

	"github.com/onedaycat/zamus/eventstore"
)

type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

type LambdaHandler func(ctx context.Context, event *DynamoDBStreamEvent) (interface{}, error)
type EventMessageHandler func(msg *EventMsg) error
type EventMessagesHandler func(msgs EventMsgs) error
type EventMessageErrorHandler func(msg *EventMsg, err error)
type EventMessagesErrorHandler func(msgs EventMsgs, err error)
type KeyHandler func(record *Record) string

type DyanmoStream struct{}

func New() *DyanmoStream {
	return &DyanmoStream{}
}

func (s *DyanmoStream) CreateIteratorHandler(handler EventMessageHandler, onError EventMessageErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *DynamoDBStreamEvent) (interface{}, error) {
		if handler == nil {
			return nil, nil
		}
		if onError == nil {
			onError = func(msg *EventMsg, err error) {}
		}

		var err error
		var msg *EventMsg
		for _, record := range event.Records {
			if record.EventName != EventInsert {
				continue
			}

			msg = record.DynamoDB.NewImage.EventMsg
			if err = handler(record.DynamoDB.NewImage.EventMsg); err != nil {
				onError(msg, err)
			}
		}

		return nil, nil
	}
}

func (s *DyanmoStream) CreateConcurencyHandler(getKey KeyHandler, handler EventMessageHandler, onError EventMessageErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *DynamoDBStreamEvent) (interface{}, error) {
		if handler == nil {
			return nil, nil
		}
		if onError == nil {
			onError = func(msg *EventMsg, err error) {}
		}

		cm := newConcurrencyManager(len(event.Records))

		for _, record := range event.Records {
			if record.EventName != EventInsert {
				cm.wg.Done()
				continue
			}

			cm.Send(record, getKey, handler, onError)
		}

		cm.Wait()

		return nil, nil
	}
}

func (s *DyanmoStream) CreateGroupConcurencyHandler(getKey KeyHandler, handler EventMessagesHandler, onError EventMessagesErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *DynamoDBStreamEvent) (interface{}, error) {
		if handler == nil {
			return nil, nil
		}
		if onError == nil {
			onError = func(msgs EventMsgs, err error) {}
		}

		cm := NewGroupConcurrency()

		// cm.Send(event.Records, getKey, handler, onError)
		cm.Wait()

		return nil, nil
	}
}
