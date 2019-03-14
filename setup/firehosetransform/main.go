package main

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/zamus/eventstore"
)

func handler(ctx context.Context, evs *events.KinesisFirehoseEvent) (*events.KinesisFirehoseResponse, error) {
	var raw []byte
	var eventmsg *eventstore.EventMsg
	var err error
	result := make([]events.KinesisFirehoseResponseRecord, len(evs.Records))

	for i, record := range evs.Records {
		eventmsg = &eventstore.EventMsg{}
		if err = eventmsg.Unmarshal(record.Data); err != nil {
			result[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
				Data:     record.Data,
			}
			continue
		}

		raw, err = jsoniter.ConfigFastest.Marshal(eventmsg)
		if err != nil {
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
			Data:     raw,
		}
	}

	return &events.KinesisFirehoseResponse{result}, nil
}

func main() {
	lambda.Start(handler)
}
