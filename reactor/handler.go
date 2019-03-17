package reactor

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/reactor/kinesisstream"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type EventHandler = kinesisstream.EventMessagesHandler
type ErrorHandler = kinesisstream.EventMessagesErrorHandler
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg
type LambdaEvent = kinesisstream.KinesisStreamEvent

type Config struct {
	AppStage            string
	Service             string
	Version             string
	SentryRelease       string
	SentryDNS           string
	DisableReponseError bool
	EnableTrace         bool
	DQLMaxRetry         int
	DQLStorage          dql.Storage
}

type Handler struct {
	streamer            kinesisstream.KinesisHandlerStrategy
	zcctx               *zamuscontext.ZamusContext
	warmer              *warmer.Warmer
	disableReponseError bool
}

func NewHandler(streamer kinesisstream.KinesisHandlerStrategy, config *Config) *Handler {
	h := &Handler{
		zcctx: &zamuscontext.ZamusContext{
			AppStage:       config.AppStage,
			Service:        config.Service,
			LambdaFunction: lambdacontext.FunctionName,
			LambdaVersion:  lambdacontext.FunctionVersion,
			Version:        config.Version,
		},
		streamer:            streamer,
		disableReponseError: config.DisableReponseError,
	}

	if config.DQLMaxRetry > 0 && config.DQLStorage != nil {
		h.streamer.SetDQL(dql.New(config.DQLStorage, config.DQLMaxRetry, config.Service, lambdacontext.FunctionName, config.Version))
	}

	if config.EnableTrace {
		tracer.Enable = config.EnableTrace
		h.ErrorHandlers(TraceError)
	}

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
		h.ErrorHandlers(Sentry)
	}

	return h
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

func (h *Handler) RegisterHandler(handler EventHandler, filterEvents []string) {
	h.streamer.RegisterHandler(handler, filterEvents)
}

func (h *Handler) Handle(ctx context.Context, event *LambdaEvent) errors.Error {
	if event.Warmer {
		return h.runWarmer(ctx, event)
	}
	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	return h.streamer.Process(zmctx, event.Records)
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	req := &LambdaEvent{}
	if err := common.UnmarshalJSON(payload, req); err != nil {
		return nil, err
	}

	if h.disableReponseError {
		h.Handle(ctx, req)
		return nil, nil
	}

	return nil, h.Handle(ctx, req)
}

func (h *Handler) StartLambda() {
	lambda.StartHandler(h)
}

func (h *Handler) runWarmer(ctx context.Context, event *LambdaEvent) errors.Error {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}
		h.warmer = warmer.New(sess)
	}
	h.warmer.Run(ctx, event.Concurency)

	return nil
}