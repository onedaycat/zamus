package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
)

type Handler struct {
}

func (h *Handler) Handle(ctx context.Context, event *events.KinesisFirehoseEvent) (*events.KinesisFirehoseResponse, errors.Error) {
	var eventmsg *eventstore.EventMsg
	var data []byte
	var err error
	result := make([]events.KinesisFirehoseResponseRecord, len(event.Records))

	for i, record := range event.Records {
		eventmsg = &eventstore.EventMsg{}
		if err = eventmsg.Unmarshal(record.Data); err != nil {
			xerr := appErr.ErrUnableDecode.WithCaller().WithCause(err).WithInput(record.Data)
			Sentry(ctx, event, xerr)
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
			Sentry(ctx, event, xerr)
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

	return &events.KinesisFirehoseResponse{result}, nil
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
			{"lambdaVersion", lambdacontext.FunctionVersion},
		}),
	)

	h := &Handler{}
	lambda.StartHandler(h)
}
