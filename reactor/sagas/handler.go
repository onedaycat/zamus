package sagas

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/dynamostream"
)

type EventHandler = dynamostream.EventMessagesHandler
type ErrorHandler = dynamostream.EventMessagesErrorHandler
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg
type LambdaEvent = dynamostream.DynamoDBStreamEvent

type Handler struct {
	gropcon *dynamostream.GroupConcurrency
}

func NewHandler() *Handler {
	return &Handler{
		gropcon: dynamostream.NewGroupConcurrency(),
	}
}

func (h *Handler) PreHandler(handlers ...EventHandler) {
	h.gropcon.PreHandlers(handlers...)
}

func (h *Handler) PostHandler(handlers ...EventHandler) {
	h.gropcon.PostHandlers(handlers...)
}

func (h *Handler) ErrorHandler(handlers ...ErrorHandler) {
	h.gropcon.ErrorHandlers(handlers...)
}

func (h *Handler) RegisterHandler(handler EventHandler) {
	h.gropcon.RegisterHandler(handler)
}

func (h *Handler) FilterEvents(eventTypes ...string) {
	h.gropcon.FilterEvents(eventTypes...)
}

func (h *Handler) handler(ctx context.Context, event *LambdaEvent) {
	h.gropcon.Process(event.Records)
	h.gropcon.Wait()
}

func (h *Handler) StartLambda() {
	lambda.Start(h.handler)
}
