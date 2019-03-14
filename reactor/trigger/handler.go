package trigger

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/dql"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/zamuscontext"
)

var json = jsoniter.ConfigFastest

type TriggerHandler = func(ctx context.Context, payload Payload) (interface{}, errors.Error)
type ErrorHandler = func(ctx context.Context, payload Payload, err errors.Error)

type Payload jsoniter.RawMessage

func (p Payload) Unmarshal(v interface{}) errors.Error {
	if err := json.Unmarshal(p, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(p)
	}

	return nil
}

func (p Payload) Marshal(v interface{}) errors.Error {
	var err error
	p, err = json.Marshal(v)
	if err != nil {
		return appErr.ErrUnableMarshal.WithCause(err).WithCaller().WithInput(v)
	}

	return nil
}

type Config struct {
	AppStage      string
	Service       string
	Version       string
	SentryRelease string
	SentryDNS     string
	EnableTrace   bool
	DQLMaxRetry   int
	DQLStorage    dql.Storage
}

type Handler struct {
	errorhandlers []ErrorHandler
	handler       TriggerHandler
	zcctx         *zamuscontext.ZamusContext
	dql           dql.DQL
}

func NewHandler(handler TriggerHandler, config *Config) *Handler {
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

	h := &Handler{
		zcctx: &zamuscontext.ZamusContext{
			AppStage:       config.AppStage,
			Service:        config.Service,
			LambdaFunction: lambdacontext.FunctionName,
			LambdaVersion:  lambdacontext.FunctionVersion,
			Version:        config.Version,
		},
		handler: handler,
	}

	if config.DQLMaxRetry > 0 && config.DQLStorage != nil {
		h.dql = dql.New(config.DQLStorage, config.DQLMaxRetry, config.Service, lambdacontext.FunctionName, config.Version)
	}

	return h
}

func (h *Handler) ErrorHandlers(handlers ...ErrorHandler) {
	h.errorhandlers = handlers
}

func (h *Handler) Handle(ctx context.Context, payload Payload) (result interface{}, err errors.Error) {
	zmctx := zamuscontext.NewContext(ctx, h.zcctx)
	defer h.recovery(zmctx, payload, &err)

DQLRetry:
	result, err = h.handler(zmctx, payload)
	if err != nil {
		for _, errhandler := range h.errorhandlers {
			errhandler(zmctx, payload, err)
		}

		if h.dql != nil {
			if ok := h.dql.Retry(); ok {
				goto DQLRetry
			}

			if xerr := h.dql.Save(zmctx, payload); xerr != nil {
				return nil, xerr
			}
		}

		return nil, err
	}

	return result, nil
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) (resultByte []byte, err error) {
	result, err := h.Handle(ctx, Payload(payload))
	if err != nil {
		return nil, err
	}

	resultByte, _ = json.Marshal(result)

	return resultByte, nil
}

func (h *Handler) StartLambda() {
	lambda.StartHandler(h)
}

func (h *Handler) recovery(ctx context.Context, payload Payload, err *errors.Error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrInternalError.WithCause(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errorhandlers {
				errhandler(ctx, payload, *err)
			}
			tracer.AddError(ctx, *err)
		default:
			*err = appErr.ErrInternalError.WithInput(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errorhandlers {
				errhandler(ctx, payload, *err)
			}
			tracer.AddError(ctx, *err)
		}
	}
}
