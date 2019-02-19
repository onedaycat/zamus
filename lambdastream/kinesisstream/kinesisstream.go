package kinesisstream

import (
	"context"

	"github.com/onedaycat/zamus/eventstore"
)

type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

type LambdaHandler func(ctx context.Context, event *KinesisStreamEvent)
type EventMessageHandler func(msg *EventMsg) error
type EventMessagesHandler func(msgs EventMsgs) (*EventMsg, error)
type EventMessageErrorHandler func(msg *EventMsg, err error)

type KinesisStream struct{}

func New() *KinesisStream {
	return &KinesisStream{}
}

func (s *KinesisStream) CreateIteratorHandler(handler EventMessageHandler, onError EventMessageErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *KinesisStreamEvent) {
		if handler == nil {
			return
		}
		if onError == nil {
			onError = func(msg *EventMsg, err error) {}
		}

		var err error
		var msg *eventstore.EventMsg
		for _, record := range event.Records {
			msg = record.Kinesis.Data.EventMsg
			if err = handler(record.Kinesis.Data.EventMsg); err != nil {
				onError(msg, err)
			}
		}
	}
}

func (s *KinesisStream) CreateConcurencyHandler(handler EventMessageHandler, onError EventMessageErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *KinesisStreamEvent) {
		if handler == nil {
			return
		}
		if onError == nil {
			onError = func(msg *EventMsg, err error) {}
		}

		cm := newConcurrencyManager(len(event.Records))

		for _, record := range event.Records {
			cm.Send(record, handler, onError)
		}

		cm.Wait()
	}
}

func (s *KinesisStream) CreateGroupConcurencyHandler(handler EventMessagesHandler, onError EventMessageErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *KinesisStreamEvent) {
		if handler == nil {
			return
		}
		if onError == nil {
			onError = func(msg *EventMsg, err error) {}
		}

		cm := NewGroupConcurrency()

		cm.Process(event.Records)
		cm.Wait()
	}
}
