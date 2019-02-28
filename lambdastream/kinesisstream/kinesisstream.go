package kinesisstream

import (
	"context"

	"github.com/onedaycat/zamus/eventstore"
)

type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

type LambdaHandler func(ctx context.Context, event *KinesisStreamEvent)
type EventMessageHandler func(msg *EventMsg) error
type EventMessagesHandler func(msgs EventMsgs) error
type EventMessageErrorHandler func(msg *EventMsg, err error)
type EventMessagesErrorHandler func(msgs EventMsgs, err error)

type KinesisHandlerStrategy interface {
	ErrorHandlers(handlers ...EventMessagesErrorHandler)
	FilterEvents(eventTypes ...string)
	PreHandlers(handlers ...EventMessagesHandler)
	PostHandlers(handlers ...EventMessagesHandler)
	RegisterHandlers(handlers ...EventMessagesHandler)
	Process(records Records) error
}
