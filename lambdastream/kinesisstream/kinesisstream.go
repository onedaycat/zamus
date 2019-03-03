package kinesisstream

import (
	"context"

	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
)

type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

type LambdaHandler func(ctx context.Context, event *KinesisStreamEvent)
type EventMessagesHandler func(ctx context.Context, msgs EventMsgs) errors.Error
type EventMessagesErrorHandler func(ctx context.Context, msgs EventMsgs, err errors.Error)

type KinesisHandlerStrategy interface {
	ErrorHandlers(handlers ...EventMessagesErrorHandler)
	FilterEvents(eventTypes ...string)
	PreHandlers(handlers ...EventMessagesHandler)
	PostHandlers(handlers ...EventMessagesHandler)
	RegisterHandlers(handlers ...EventMessagesHandler)
	Process(ctx context.Context, records Records) errors.Error
	SetDQL(dql dql.DQL)
}
