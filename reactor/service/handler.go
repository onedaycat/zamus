package service

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
)

type EventHandler = kinesisstream.EventMessagesHandler
type ErrorHandler = kinesisstream.EventMessageErrorHandler

type Handler struct {
	gropcon      *kinesisstream.GroupConcurrency
	preHandlers  []kinesisstream.EventMessagesHandler
	postHandlers []kinesisstream.EventMessagesHandler
}

func NewHandler() *Handler {
	return &Handler{
		gropcon: kinesisstream.NewGroupConcurrency(),
	}
}

func (h *Handler) PreHandler(handlers ...EventHandler) {
	h.preHandlers = handlers
}

func (h *Handler) PostHandler(handlers ...EventHandler) {
	h.postHandlers = handlers
}

func (h *Handler) ErrorHandler(handlers ...ErrorHandler) {
	h.gropcon.ErrorHandler(handlers...)
}

func (h *Handler) RegisterEvent(eventType string, handler EventHandler) {
	h.gropcon.RegisterEvent(eventType, handler)
}

func (h *Handler) handler(ctx context.Context, event *kinesisstream.KinesisStreamEvent) {
	h.gropcon.Process(event.Records)
	h.gropcon.Wait()
}

func (h *Handler) StartLambda() {
	lambda.Start(h.handler)
}
