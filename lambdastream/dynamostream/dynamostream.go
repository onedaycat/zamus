package dynamostream

import (
	"context"

	"github.com/onedaycat/gocqrs"
)

type EventMessage = gocqrs.EventMessage
type EventMessages = []*gocqrs.EventMessage

type LambdaHandler func(ctx context.Context, event *DynamoDBStreamEvent) (interface{}, error)
type EventMessageHandler func(msg *EventMessage) error
type EventMessagesHandler func(msgs EventMessages) error
type EventMessageErrorHandler func(msg *EventMessage, err error)
type EventMessagesErrorHandler func(msgs EventMessages, err error)
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
			onError = func(msg *EventMessage, err error) {}
		}

		var err error
		var msg *EventMessage
		for _, record := range event.Records {
			if record.EventName != EventInsert {
				continue
			}

			msg = record.DynamoDB.NewImage.EventMessage
			if err = handler(record.DynamoDB.NewImage.EventMessage); err != nil {
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
			onError = func(msg *EventMessage, err error) {}
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
			onError = func(msgs EventMessages, err error) {}
		}

		cm := newGroupConcurrencyManager(len(event.Records))

		cm.Send(event.Records, getKey, handler, onError)
		cm.Wait()

		return nil, nil
	}
}
