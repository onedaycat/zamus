package main

import (
	"context"
	"errors"
	"math"
	"os"
	"runtime"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/gocqrs"
	"github.com/onedaycat/gocqrs/lambdastream/dynamostream"
	"github.com/rs/zerolog/log"
)

var (
	ks         *kinesis.Kinesis
	streamName = os.Getenv("KINESIS_STREAM_NAME")
	numCore    = runtime.NumCPU() * 2
)

func work(records dynamostream.Records, result *[]*kinesis.PutRecordsRequestEntry, wg *sync.WaitGroup) {
	var event *gocqrs.EventMessage
	for i := 0; i < len(records); i++ {
		if records[i].DynamoDB.NewImage == nil {
			continue
		}
		event = records[i].DynamoDB.NewImage.EventMessage

		data, _ := event.Marshal()
		*result = append(*result, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &event.PartitionKey,
		})
	}

	wg.Done()
}

func handler(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) error {
	n := len(stream.Records)
	if n < numCore {
		return handlerIterator(ctx, stream)
	}

	wg := &sync.WaitGroup{}
	wg.Add(numCore)
	numOfWork := int(math.Ceil(float64(n) / float64(numCore)))
	dataSets := make([][]*kinesis.PutRecordsRequestEntry, numCore)

	for i := 0; i < numCore; i++ {
		dataSets[i] = make([]*kinesis.PutRecordsRequestEntry, 0, numOfWork)
		if (i+1)*numOfWork > n {
			go work(stream.Records[i*numOfWork:n], &dataSets[i], wg)
			break
		}
		go work(stream.Records[i*numOfWork:(i+1)*numOfWork], &dataSets[i], wg)
	}
	wg.Wait()

	result := make([]*kinesis.PutRecordsRequestEntry, 0, n)

	for i := 0; i < numCore; i++ {
		result = append(result, dataSets[i]...)
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

func handlerIterator(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) error {
	n := len(stream.Records)
	result := make([]*kinesis.PutRecordsRequestEntry, 0, n)

	var event *gocqrs.EventMessage
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
	lambda.Start(handler)
}
