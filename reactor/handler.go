package reactor

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	ldService "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/dlq"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type EventHandler = func(ctx context.Context, msgs event.Msgs) errors.Error
type ErrorHandler = func(ctx context.Context, msgs event.Msgs, err errors.Error)
type LambdaHandler func(ctx context.Context, event event.Msgs)

type Strategy interface {
	ErrorHandlers(handlers ...ErrorHandler)
	PreHandlers(handlers ...EventHandler)
	PostHandlers(handlers ...EventHandler)
	RegisterHandler(handlers EventHandler, filterEvents []string)
	Process(ctx context.Context, msgs event.Msgs) errors.Error
	SetDLQ(dlq dlq.DLQ)
}

type EventSource interface {
	GetRequest(ctx context.Context, payload []byte) (*Request, errors.Error)
}

type Request struct {
	Msgs       event.Msgs
	Warmer     bool
	Concurency int
}

type Config struct {
	AppStage            string
	Service             string
	Version             string
	SentryDSN           string
	DisableReponseError bool
	EnableTrace         bool
	DLQMaxRetry         int
	DLQStorage          dlq.Storage
}

type Handler struct {
	streamer            Strategy
	source              EventSource
	zcctx               *zamuscontext.ZamusContext
	warmer              *warmer.Warmer
	disableReponseError bool
}

//noinspection GoUnusedExportedFunction
func NewHandler(source EventSource, streamer Strategy, config *Config) *Handler {
	h := &Handler{
		zcctx: &zamuscontext.ZamusContext{
			AppStage:       config.AppStage,
			Service:        config.Service,
			LambdaFunction: lambdacontext.FunctionName,
			LambdaVersion:  lambdacontext.FunctionVersion,
			Version:        config.Version,
		},
		streamer:            streamer,
		source:              source,
		disableReponseError: config.DisableReponseError,
	}

	if config.DLQMaxRetry > 0 && config.DLQStorage != nil {
		h.streamer.SetDLQ(dlq.New(config.DLQStorage, config.DLQMaxRetry, config.Service, lambdacontext.FunctionName, config.Version))
	}

	if tracer.Enable {
		h.ErrorHandlers(TraceError)
	}

	if config.SentryDSN != "" {
		sentry.SetDSN(config.SentryDSN)
		sentry.SetOptions(
			sentry.WithEnv(config.AppStage),
			sentry.WithServerName(lambdacontext.FunctionName),
			sentry.WithServiceName(config.Service),
			sentry.WithRelease(config.Service+"@"+config.Version),
			sentry.WithVersion(config.Version),
			sentry.WithTags(sentry.Tags{
				{Key: "lambdaVersion", Value: lambdacontext.FunctionVersion},
			}),
		)
		h.ErrorHandlers(Sentry)
	}

	return h
}

func (h *Handler) StreamStrategy(streamStrategy Strategy) {
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

func (h *Handler) Handle(ctx context.Context, req *Request) errors.Error {
	if req.Warmer {
		return h.runWarmer(ctx, req)
	}
	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	return h.streamer.Process(zmctx, req.Msgs)
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := h.source.GetRequest(ctx, payload)
	if err != nil {
		return nil, appErr.ToLambdaError(err)
	}

	if h.disableReponseError {
		_ = h.Handle(ctx, req)
		return nil, nil
	}

	return nil, appErr.ToLambdaError(h.Handle(ctx, req))
}

func (h *Handler) StartLambda() {
	lambda.StartHandler(h)
}

func (h *Handler) runWarmer(ctx context.Context, req *Request) errors.Error {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}

		h.warmer = warmer.New(ldService.New(sess))
	}
	h.warmer.Run(ctx, req.Concurency)

	return nil
}
