package sagas

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
)

type EventHandler = kinesisstream.EventMessagesHandler
type ErrorHandler = kinesisstream.EventMessagesErrorHandler
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg
type LambdaEvent = kinesisstream.KinesisStreamEvent

func init() {
	common.PrettyLog()
	xray.SetLogger(xraylog.NullLogger)
}

type Handler struct {
	streamer kinesisstream.KinesisHandlerStrategy
}

func NewHandler(streamer kinesisstream.KinesisHandlerStrategy) *Handler {
	return &Handler{
		streamer: streamer,
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
	h.streamer.Process(ctx, event.Records)
}

func (h *Handler) StartLambda() {
	lambda.Start(h.Handle)
}
