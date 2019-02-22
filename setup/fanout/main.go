package main

import (
	"context"
	"errors"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/dynamostream"
	"github.com/rs/zerolog/log"
)

var (
	streamName = os.Getenv("KINESIS_STREAM_NAME")
)

func handlerIterator(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) error {
	n := len(stream.Records)
	result := make([]*kinesis.PutRecordsRequestEntry, 0, n)

	var event *eventstore.EventMsg
	for i := 0; i < n; i++ {
		if stream.Records[i].DynamoDB.NewImage == nil {
			continue
		}
		event = stream.Records[i].DynamoDB.NewImage.EventMessage

		data, _ := event.Marshal()
		result = append(result, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &event.PartitionKey,
		})
	}

	if len(result) == 0 {
		return nil
	}

	out, err := ks.PutRecords(&kinesis.PutRecordsInput{
		Records:    result,
		StreamName: &streamName,
	})

	if err != nil {
		return err
	}

	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		return errors.New("One or more events published failed")
	}

	return nil
}

func init() {
	sess, err := session.NewSession()
	if err != nil {
		log.Panic().Msg("AWS Session error: " + err.Error())
	}

	ks = kinesis.New(sess)
}

func main() {
	lambda.Start(handlerIterator)
}
