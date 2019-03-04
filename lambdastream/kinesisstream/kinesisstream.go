package kinesisstream

import (
	"context"

	"github.com/onedaycat/zamus/common"
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
	RegisterHandler(handlers EventMessagesHandler, filterEvents ...string)
	Process(ctx context.Context, records Records) errors.Error
	SetDQL(dql dql.DQL)
}

type Handler struct {
	Handler      EventMessagesHandler
	FilterEvents common.Set
}
