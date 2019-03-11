package sideeffect

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type EventHandler = kinesisstream.EventMessagesHandler
type ErrorHandler = kinesisstream.EventMessagesErrorHandler
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg
type LambdaEvent = kinesisstream.KinesisStreamEvent
type FilterEvents = kinesisstream.FilterEvents

type Config struct {
	AppStage         string
	Service          string
	Version          string
	SentryRelease    string
	SentryDNS        string
	EnableTrace      bool
	DQLMaxRetry      int
	DQLStorage       dql.Storage
	WarmerConcurency int
}

type Handler struct {
	streamer         kinesisstream.KinesisHandlerStrategy
	zcctx            *zamuscontext.ZamusContext
	warmer           *warmer.Warmer
	warmerConcurency int
}

func NewHandler(streamer kinesisstream.KinesisHandlerStrategy, config *Config) *Handler {
	if config.SentryDNS != "" {
		sentry.SetDSN(config.SentryDNS)
		sentry.SetOptions(
			sentry.WithEnv(config.AppStage),
			sentry.WithRelease(config.SentryRelease),
			sentry.WithServerName(lambdacontext.FunctionName),
			sentry.WithServiceName(config.Service),
			sentry.WithVersion(config.Version),
			sentry.WithTags(sentry.Tags{
				{"lambdaVersion", lambdacontext.FunctionVersion},
			}),
		)
	}

	tracer.Enable = config.EnableTrace

	if config.DQLMaxRetry > 0 && config.DQLStorage != nil {
		streamer.SetDQL(dql.New(config.DQLStorage, config.DQLMaxRetry, config.Service, lambdacontext.FunctionName, config.Version))
	}

	return &Handler{
		zcctx: &zamuscontext.ZamusContext{
			AppStage:       config.AppStage,
			Service:        config.Service,
			LambdaFunction: lambdacontext.FunctionName,
			LambdaVersion:  lambdacontext.FunctionVersion,
			Version:        config.Version,
		},
		streamer:         streamer,
		warmerConcurency: config.WarmerConcurency,
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

func (h *Handler) RegisterHandler(handler EventHandler, filterEvents FilterEvents) {
	h.streamer.RegisterHandler(handler, filterEvents)
}

func (h *Handler) FilterEvents(eventTypes ...string) {
	h.streamer.FilterEvents(eventTypes...)
}

func (h *Handler) Handle(ctx context.Context, event *LambdaEvent) {
	if event.Warmer {
		return
	}
	h.streamer.Process(ctx, event.Records)
}

func (h *Handler) StartLambda() {
	lambda.Start(h.Handle)
}

func (h *Handler) runWarmer(ctx context.Context) errors.Error {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}
		h.warmer = warmer.New(sess, h.warmerConcurency)
	}
	h.warmer.Run(ctx)

	return nil
}
