package trigger

import (
    "context"
    "fmt"

    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-lambda-go/lambdacontext"
    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/sentry"
    "github.com/onedaycat/zamus/dlq"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/tracer"
    "github.com/onedaycat/zamus/zamuscontext"
)

//noinspection GoNameStartsWithPackageName
type TriggerHandler = func(ctx context.Context, payload Payload) (interface{}, errors.Error)
type ErrorHandler = func(ctx context.Context, payload Payload, err errors.Error)
type Payload jsoniter.RawMessage

func (p Payload) Unmarshal(v interface{}) errors.Error {
    if err := common.UnmarshalJSON(p, v); err != nil {
        return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(p)
    }

    return nil
}

type Config struct {
    AppStage      string
    Service       string
    Version       string
    SentryDSN     string
    DLQMaxRetry   int
    DLQSource     string
    DLQSourceType dlq.SourceType
    DLQStorage    dlq.Storage
}

type Handler struct {
    errorhandlers []ErrorHandler
    handler       TriggerHandler
    zcctx         *zamuscontext.ZamusContext
    dlq           dlq.DLQ
}

//noinspection GoUnusedExportedFunction
func NewHandler(handler TriggerHandler, config *Config) *Handler {
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

    if config.DLQMaxRetry > 0 && config.DLQStorage != nil {
        h.dlq = dlq.New(config.DLQStorage, &dlq.Config{
            Service:    config.Service,
            Source:     config.DLQSource,
            SourceType: config.DLQSourceType,
            MaxRetry:   config.DLQMaxRetry,
            Version:    config.Version,
        })
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

func (h *Handler) ErrorHandlers(handlers ...ErrorHandler) {
    h.errorhandlers = append(h.errorhandlers, handlers...)
}

func (h *Handler) Handle(ctx context.Context, payload Payload) (result interface{}, err errors.Error) {
    zmctx := zamuscontext.NewContext(ctx, h.zcctx)
    defer h.recovery(zmctx, payload, &err)

DLQRetry:
    result, err = h.handler(zmctx, payload)
    if err != nil {
        for _, errhandler := range h.errorhandlers {
            errhandler(zmctx, payload, err)
        }

        if h.dlq != nil {
            if ok := h.dlq.Retry(); ok {
                goto DLQRetry
            }

            if xerr := h.dlq.Save(zmctx, payload); xerr != nil {
                return nil, xerr
            }
        }

        return nil, err
    }

    return result, nil
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
    result, err := h.Handle(ctx, Payload(payload))
    if err != nil {
        return nil, appErr.ToLambdaError(err)
    }

    resultByte, _ := common.MarshalJSON(result)

    return resultByte, nil
}

func (h *Handler) StartLambda() {
    lambda.StartHandler(h)
}

func (h *Handler) recovery(ctx context.Context, payload Payload, err *errors.Error) {
    if r := recover(); r != nil {
        switch cause := r.(type) {
        case error:
            *err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(payload)
            for _, errhandler := range h.errorhandlers {
                errhandler(ctx, payload, *err)
            }
        default:
            *err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(payload)
            for _, errhandler := range h.errorhandlers {
                errhandler(ctx, payload, *err)
            }
        }
    }
}
