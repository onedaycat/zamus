package main

import (
	"context"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	ldService "github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/errors/sentry"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/reactor/dynamostream"
	"github.com/onedaycat/zamus/warmer"
	"github.com/rs/zerolog/log"
)

const (
	delim = ","
)

var json = jsoniter.ConfigFastest

type Handler struct {
	records    []*kinesis.PutRecordsRequestEntry
	streamList []string
	ks         *kinesis.Kinesis
	wrm        *warmer.Warmer
	count      int
}

func (h *Handler) publish(ctx context.Context, streamName string) errors.Error {
	out, err := h.ks.PutRecordsWithContext(ctx, &kinesis.PutRecordsInput{
		Records:    h.records[:h.count],
		StreamName: &streamName,
	})

	if err != nil {
		appErr := appErr.ErrUnablePublishKinesis.WithCaller().WithCause(err)
		return appErr
	}

	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		appErr := appErr.ErrUnablePublishKinesis.WithCaller().WithCause(errors.New("One or more events published failed"))
		return appErr
	}

	return nil
}

func (h *Handler) Process(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) errors.Error {
	records := stream.Records
	h.count = 0

	for i := 0; i < len(records); i++ {
		if records[i].EventName != dynamostream.EventInsert || records[i].DynamoDB.NewImage == nil {
			continue
		}

		data, _ := records[i].DynamoDB.NewImage.EventMsg.Marshal()
		if len(h.records) <= h.count {
			h.records = append(h.records, &kinesis.PutRecordsRequestEntry{
				Data:         data,
				PartitionKey: &records[i].DynamoDB.NewImage.EventMsg.AggregateID,
			})
		} else {
			h.records[h.count].Data = data
			h.records[h.count].PartitionKey = &records[i].DynamoDB.NewImage.EventMsg.AggregateID
		}
		h.count++
	}

	return nil
}

func (h *Handler) Handle(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) errors.Error {
	if stream.Warmer {
		h.wrm.Run(ctx, stream.Concurency)
		return nil
	}

	if err := h.Process(ctx, stream); err != nil {
		return err
	}

	if h.count == 0 {
		return nil
	}

	wg := errgroup.Group{}
	for _, streamName := range h.streamList {
		wg.Go(func() errors.Error {
			if err := h.publish(ctx, streamName); err != nil {
				return err
			}

			return nil
		})
	}

	return wg.Wait()
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	stream := &dynamostream.DynamoDBStreamEvent{}
	if err := json.Unmarshal(payload, stream); err != nil {
		xerr := appErr.ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(payload)
		Sentry(ctx, stream, xerr)
		return nil, xerr
	}

	if xerr := h.Handle(ctx, stream); xerr != nil {
		Sentry(ctx, stream, xerr)
		return nil, xerr
	}

	return nil, nil
}

func main() {
	dsn := os.Getenv("APP_SENTRY_DSN")
	stage := os.Getenv("APP_STAGE")
	streamNames := os.Getenv("KINESIS_STREAM_NAMES")

	sentry.SetDSN(dsn)
	sentry.SetOptions(
		sentry.WithEnv(stage),
		sentry.WithServerName(lambdacontext.FunctionName),
		sentry.WithServiceName("dynamodb-kinesis"),
		sentry.WithTags(sentry.Tags{
			{"lambdaVersion", lambdacontext.FunctionVersion},
		}),
	)

	sess, err := session.NewSession()
	if err != nil {
		log.Panic().Msg("AWS Session error: " + err.Error())
	}

	dh := &Handler{
		records:    make([]*kinesis.PutRecordsRequestEntry, 0, 200),
		wrm:        warmer.New(ldService.New(sess)),
		streamList: strings.Split(streamNames, delim),
		ks:         kinesis.New(sess),
	}

	lambda.StartHandler(dh)
}
