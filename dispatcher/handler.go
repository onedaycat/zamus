package dispatcher

import (
    "context"
    "fmt"

    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-lambda-go/lambdacontext"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/request"
    "github.com/aws/aws-sdk-go/service/kinesis"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/errgroup"
    "github.com/onedaycat/errors/sentry"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/invoke"
    "github.com/onedaycat/zamus/reactor/dynamostream"
    "github.com/onedaycat/zamus/zamuscontext"
)

var (
    ErrUnableUnmarshal      = errors.DefInternalError("ErrUnableUnmarshal", "Unable unmarshal json")
    ErrUnablePublishKinesis = errors.DefInternalError("ErrUnablePublishKinesis", "Unable to publish kinesis")
    ErrUnablePublishSNS     = errors.DefInternalError("ErrUnablePublishSNS", "Unable to publish sns")
    ErrUnablePublishSQS     = errors.DefInternalError("ErrUnablePublishSQS", "Unable to publish sqs")
)

//go:generate mockery -name=KinesisPublisher
type KinesisPublisher interface {
    PutRecordsWithContext(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error)
}

type desConfig interface {
    filter(msg *event.Msg)
    clear()
    hasEvents() bool
    publish() errors.Error
    setContext(ctx context.Context)
}

type Config struct {
    AppStage  string
    Service   string
    Version   string
    SentryDSN string
    //DLQMaxRetry         int
    //DLQStorage          dlq.Storage
}

type Handler struct {
    desconfig []desConfig
    wgPublish *errgroup.Group
    invoker   invoke.Invoker
    kinClient KinesisPublisher
    recs      *dynamostream.EventSource
    zcctx     *zamuscontext.ZamusContext
}

func New(config *Config) *Handler {
    h := &Handler{
        desconfig: make([]desConfig, 0, 20),
        wgPublish: &errgroup.Group{},
        recs: &dynamostream.EventSource{
            Records: make(dynamostream.Records, 0, 100),
        },
        zcctx: &zamuscontext.ZamusContext{
            AppStage:       config.AppStage,
            Service:        config.Service,
            LambdaFunction: lambdacontext.FunctionName,
            LambdaVersion:  lambdacontext.FunctionVersion,
            Version:        config.Version,
        },
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
    }

    return h
}

func (h *Handler) Lambda(config *LambdaConfig) {
    config.init()
    h.desconfig = append(h.desconfig, config)
}

func (h *Handler) Saga(config *SagaConfig) {
    config.init()
    h.desconfig = append(h.desconfig, config)
}

func (h *Handler) Kinesis(config *KinesisConfig) {
    config.init()
    h.desconfig = append(h.desconfig, config)
}

func (h *Handler) SNS(config *SNSConfig) {
    config.init()
    h.desconfig = append(h.desconfig, config)
}

func (h *Handler) SQS(config *SQSConfig) {
    config.init()
    h.desconfig = append(h.desconfig, config)
}

func (h *Handler) Handle(ctx context.Context, stream *dynamostream.EventSource) (err errors.Error) {
    defer h.recovery(ctx, &err)
    for _, conf := range h.desconfig {
        conf.clear()
        conf.setContext(ctx)
    }

    for _, rec := range stream.Records {
        if rec.EventName != dynamostream.EventInsert || rec.DynamoDB.NewImage == nil {
            continue
        }

        for _, conf := range h.desconfig {
            conf.filter(rec.DynamoDB.NewImage.EventMsg)
        }
    }

    for _, conf := range h.desconfig {
        conf := conf
        if conf.hasEvents() {
            h.wgPublish.Go(conf.publish)
        }
    }

    return h.wgPublish.Wait()
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
    h.recs.Clear()
    if err := common.UnmarshalJSON(payload, h.recs); err != nil {
        Sentry(ctx, h.recs, err)
        return nil, appErr.ToLambdaError(err)
    }

    zmctx := zamuscontext.NewContext(ctx, h.zcctx)
    if err := h.Handle(zmctx, h.recs); err != nil {
        Sentry(ctx, h.recs, err)
        TraceError(ctx, err)
        return nil, appErr.ToLambdaError(err)
    }

    return nil, nil
}

func (h *Handler) StartLambda() {
    lambda.StartHandler(h)
}

func (h *Handler) recovery(ctx context.Context, err *errors.Error) {
    if r := recover(); r != nil {
        switch cause := r.(type) {
        case error:
            *err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(h.recs)
        default:
            *err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(h.recs)
        }
    }
}
