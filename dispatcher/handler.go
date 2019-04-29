package dispatcher

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/reactor/dynamostream"
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

type Handler struct {
	config    []desConfig
	wgPublish *errgroup.Group
	invoker   invoke.Invoker
	kinClient KinesisPublisher
	recs      *dynamostream.EventSource
}

func New(invoker invoke.Invoker, kinClient KinesisPublisher) *Handler {
	h := &Handler{
		config:    make([]desConfig, 0, 20),
		wgPublish: &errgroup.Group{},
		invoker:   invoker,
		kinClient: kinClient,
		recs: &dynamostream.EventSource{
			Records: make(dynamostream.Records, 0, 100),
		},
	}

	return h
}

func (h *Handler) Lambda(config *LambdaConfig) {
	config.init()
	config.Client = h.invoker
	h.config = append(h.config, config)
}

func (h *Handler) Saga(config *SagaConfig) {
	config.init()
	config.Client = h.invoker
	h.config = append(h.config, config)
}

func (h *Handler) Kinesis(config *KinesisConfig) {
	config.init()
	config.Client = h.kinClient
	h.config = append(h.config, config)
}

func (h *Handler) Handle(ctx context.Context, stream *dynamostream.EventSource) (err errors.Error) {
	defer h.recovery(ctx, &err)
	for _, conf := range h.config {
		conf.clear()
		conf.setContext(ctx)
	}

	for _, rec := range stream.Records {
		if rec.EventName != dynamostream.EventInsert || rec.DynamoDB.NewImage == nil {
			continue
		}

		for _, conf := range h.config {
			conf.filter(rec.DynamoDB.NewImage.EventMsg)
		}
	}

	for _, conf := range h.config {
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

	if err := h.Handle(ctx, h.recs); err != nil {
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
