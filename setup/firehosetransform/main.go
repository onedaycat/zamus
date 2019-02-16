package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/zamus"
)

func handler(ctx context.Context, evs *events.KinesisFirehoseEvent) (*events.KinesisFirehoseResponse, error) {
	var raw []byte
	var eventmsg *zamus.EventMessage
	var err error
	result := make([]events.KinesisFirehoseResponseRecord, len(evs.Records))

	for i, record := range evs.Records {
		eventmsg = &zamus.EventMessage{}
		if err = eventmsg.Unmarshal(record.Data); err != nil {
			result[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
				Data:     record.Data,
			}
			continue
		}

		raw, err = json.Marshal(eventmsg)
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
