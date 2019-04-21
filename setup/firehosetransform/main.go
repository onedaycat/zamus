package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common"
)

type Handler struct {
}

func (h *Handler) Handle(ctx context.Context, kinevt *events.KinesisFirehoseEvent) (*events.KinesisFirehoseResponse, errors.Error) {
	var eventmsg *event.Msg
	var data []byte
	var err error
	result := make([]events.KinesisFirehoseResponseRecord, len(kinevt.Records))

	for i, record := range kinevt.Records {
		eventmsg = &event.Msg{}
		if err = event.UnmarshalMsg(record.Data, eventmsg); err != nil {
			xerr := appErr.ErrUnableDecode.WithCaller().WithCause(err).WithInput(record.Data)
			Sentry(ctx, kinevt, xerr)
			result[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
				Data:     record.Data,
			}
			continue
		}

		data, err = common.MarshalJSON(eventmsg)
		if err != nil {
			xerr := appErr.ErrUnableMarshal.WithCaller().WithCause(err).WithInput(eventmsg)
			Sentry(ctx, kinevt, xerr)
			result[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
				Data:     record.Data,
			}
			continue
		}

		result[i] = events.KinesisFirehoseResponseRecord{
			RecordID: record.RecordID,
			Result:   events.KinesisFirehoseTransformedStateOk,
			Data:     data,
		}
	}

	return &events.KinesisFirehoseResponse{Records: result}, nil
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	req := &events.KinesisFirehoseEvent{}
	if err := common.UnmarshalJSON(payload, req); err != nil {
		return nil, err
	}

	result, err := h.Handle(ctx, req)
	if err != nil {
		return nil, err
	}

	resultByte, _ := common.MarshalJSON(result)

	return resultByte, nil
}

func main() {
	dsn := os.Getenv("APP_SENTRY_DSN")
	stage := os.Getenv("APP_STAGE")

	sentry.SetDSN(dsn)
	sentry.SetOptions(
		sentry.WithEnv(stage),
		sentry.WithServerName(lambdacontext.FunctionName),
		sentry.WithServiceName("firehose-transform"),
		sentry.WithTags(sentry.Tags{
			{Key: "lambdaVersion", Value: lambdacontext.FunctionVersion},
		}),
	)

	h := &Handler{}
	lambda.StartHandler(h)
}
