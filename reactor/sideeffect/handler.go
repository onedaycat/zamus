package sideeffect

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
)

type EventHandler = kinesisstream.EventMessagesHandler
type ErrorHandler = kinesisstream.EventMessagesErrorHandler
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg
type LambdaEvent = kinesisstream.KinesisStreamEvent

type Handler struct {
	gropcon *kinesisstream.GroupConcurrency
}

func NewHandler() *Handler {
	return &Handler{
		gropcon: kinesisstream.NewGroupConcurrency(),
	}
}

func (h *Handler) PreHandlers(handlers ...EventHandler) {
	h.gropcon.PreHandlers(handlers...)
}

func (h *Handler) PostHandlers(handlers ...EventHandler) {
	h.gropcon.PostHandlers(handlers...)
}

func (h *Handler) ErrorHandlers(handlers ...ErrorHandler) {
	h.gropcon.ErrorHandlers(handlers...)
}

func (h *Handler) RegisterHandlers(handlers ...EventHandler) {
	h.gropcon.RegisterHandlers(handlers...)
}

func (h *Handler) FilterEvents(eventTypes ...string) {
	h.gropcon.FilterEvents(eventTypes...)
}

func (h *Handler) handler(ctx context.Context, event *LambdaEvent) {
	h.gropcon.Process(event.Records)
}

func (h *Handler) StartLambda() {
	lambda.Start(h.handler)
}
