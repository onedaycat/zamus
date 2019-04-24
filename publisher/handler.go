package publisher

import (
    "context"

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
)

//go:generate mockery -name=KinesisPublisher
type KinesisPublisher interface {
    PutRecordsWithContext(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error)
}

type Config struct {
    Kinesis *KinesisConfig
    Fanout  *FanoutConfig
}

type KinesisConfig struct {
    Client     KinesisPublisher
    StreamARNs []string
    records    []*kinesis.PutRecordsRequestEntry
}

type FanoutConfig struct {
    Invoker      invoke.Invoker
    Lambdas      []*LambdaConfig
    lambdaEvents map[string][]int
}

type LambdaConfig struct {
    Fn      string
    Events  []string
    records *event.MsgList
}

type Handler struct {
    config   Config
    wghandle *errgroup.Group
    wgkin    *errgroup.Group
    wginvoke *errgroup.Group
    recs     *dynamostream.EventSource
}

func New(config Config) *Handler {
    h := &Handler{
        config:   config,
        wghandle: &errgroup.Group{},
        wgkin:    &errgroup.Group{},
        wginvoke: &errgroup.Group{},
        recs: &dynamostream.EventSource{
            Records: make(dynamostream.Records, 0, 100),
        },
    }

    if h.config.Kinesis != nil {
        h.config.Kinesis.records = make([]*kinesis.PutRecordsRequestEntry, 0, 100)
    }

    if h.config.Fanout != nil {
        h.config.Fanout.lambdaEvents = make(map[string][]int)
        for i, ld := range h.config.Fanout.Lambdas {
            ld.records = &event.MsgList{
                Msgs: make(event.Msgs, 0, 100),
            }

            for _, evtName := range ld.Events {
                h.config.Fanout.lambdaEvents[evtName] = append(h.config.Fanout.lambdaEvents[evtName], i)
            }
        }
    }

    return h
}

func (h *Handler) Handle(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
    if h.config.Kinesis != nil {
        h.wghandle.Go(func() errors.Error {

            if err := h.processKinesis(ctx, stream); err != nil {
                Sentry(ctx, stream, err)
                return err
            }
            return nil
        })
    }

    if h.config.Fanout != nil {
        h.wghandle.Go(func() errors.Error {
            _ = h.processInvoke(ctx, stream)
            return nil
        })
    }

    return h.wghandle.Wait()
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
    h.recs.Clear()
    if err := common.UnmarshalJSON(payload, h.recs); err != nil {
        Sentry(ctx, h.recs, err)
        return nil, appErr.ToLambdaError(err)
    }

    if err := h.Handle(ctx, h.recs); err != nil {
        Sentry(ctx, h.recs, err)
        return nil, appErr.ToLambdaError(err)
    }

    return nil, nil
}

func (h *Handler) StartLambda() {
    lambda.StartHandler(h)
}
