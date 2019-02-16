package kinesisstream

import (
	"context"

	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus"
)

type EventMessage = zamus.EventMessage
type EventMessages = []*zamus.EventMessage

type LambdaHandler func(ctx context.Context, event *KinesisStreamEvent)
type EventMessageHandler func(msg *EventMessage) error
type EventMessagesHandler func(msgs EventMessages) error
type EventMessageErrorHandler func(msg *EventMessage, err error)
type EventMessagesErrorHandler func(msgs EventMessages, err error)

type KinesisStream struct {
	enableSentry bool
}

func New() *KinesisStream {
	return &KinesisStream{}
}

func (s *KinesisStream) SetSentry(dsn string, opts ...sentry.Option) {
	s.enableSentry = true
	sentry.SetDSN(dsn)
	sentry.SetOptions(opts...)
}

func (s *KinesisStream) CreateIteratorHandler(handler EventMessageHandler, onError EventMessageErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *KinesisStreamEvent) {
		if handler == nil {
			return
		}
		if onError == nil {
			onError = func(msg *EventMessage, err error) {}
		}

		var err error
		var msg *zamus.EventMessage
		for _, record := range event.Records {
			msg = record.Kinesis.Data.EventMessage
			if err = handler(record.Kinesis.Data.EventMessage); err != nil {
				onError(msg, err)
				if s.enableSentry {
					sendSentry(ctx, msg, err)
				}
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
			onError = func(msg *EventMessage, err error) {}
		}

		cm := newConcurrencyManager(len(event.Records))

		for _, record := range event.Records {
			cm.Send(record, handler, onError)
		}

		cm.Wait()
	}
}

func (s *KinesisStream) CreateGroupConcurencyHandler(handler EventMessagesHandler, onError EventMessagesErrorHandler) LambdaHandler {
	return func(ctx context.Context, event *KinesisStreamEvent) {
		if handler == nil {
			return
		}
		if onError == nil {
			onError = func(msgs EventMessages, err error) {}
		}

		cm := newGroupConcurrencyManager(len(event.Records))

		cm.Send(event.Records, handler, onError)
		cm.Wait()
	}
}
