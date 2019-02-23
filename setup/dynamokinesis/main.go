package main

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/dynamostream"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	delim = ","
)

var (
	streamList  []string
	ks          *kinesis.Kinesis
	streamNames = os.Getenv("KINESIS_STREAM_NAMES")
)

func publish(streamName string, records []*kinesis.PutRecordsRequestEntry) error {
	out, err := ks.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
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

func handler(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) error {
	n := len(stream.Records)
	result := make([]*kinesis.PutRecordsRequestEntry, 0, n)

	var event *eventstore.EventMsg
	for i := 0; i < n; i++ {
		if stream.Records[i].EventName != dynamostream.EventInsert || stream.Records[i].DynamoDB.NewImage == nil {
			continue
		}

		event = stream.Records[i].DynamoDB.NewImage.EventMsg

		data, _ := event.Marshal()
		result = append(result, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &event.AggregateID,
		})
	}

	if len(result) == 0 {
		return nil
	}

	wg := errgroup.Group{}
	for _, streamName := range streamList {
		wg.Go(func() error {
			return publish(streamName, result)
		})
	}

	return wg.Wait()
}

func init() {
	sess, err := session.NewSession()
	if err != nil {
		log.Panic().Msg("AWS Session error: " + err.Error())
	}

	streamList = strings.Split(streamNames, delim)
	ks = kinesis.New(sess)
}

func main() {
	lambda.Start(handler)
}
