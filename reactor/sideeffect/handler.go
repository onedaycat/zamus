package sideeffect

import (
	"context"
	"runtime"

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
	streamer kinesisstream.KinesisHandlerStrategy
}

func NewHandler() *Handler {
	shard := runtime.NumCPU()
	runtime.GOMAXPROCS(shard)
	return &Handler{
		streamer: kinesisstream.NewShardStrategy(shard),
	}
}

func (h *Handler) StreamStrategy(streamStrategy kinesisstream.KinesisHandlerStrategy) {
	h.streamer = streamStrategy
}

func (h *Handler) PreHandlers(handlers ...EventHandler) {
	h.streamer.PreHandlers(handlers...)
}

func (h *Handler) PostHandlers(handlers ...EventHandler) {
	h.streamer.PostHandlers(handlers...)
}

func (h *Handler) ErrorHandlers(handlers ...ErrorHandler) {
	h.streamer.ErrorHandlers(handlers...)
}

func (h *Handler) RegisterHandlers(handlers ...EventHandler) {
	h.streamer.RegisterHandlers(handlers...)
}

func (h *Handler) FilterEvents(eventTypes ...string) {
	h.streamer.FilterEvents(eventTypes...)
}

func (h *Handler) Handle(ctx context.Context, event *LambdaEvent) {
	h.streamer.Process(event.Records)
}

func (h *Handler) StartLambda() {
	lambda.Start(h.Handle)
}
