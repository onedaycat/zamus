package main

import (
	"context"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	errs "github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/dynamostream"
	"github.com/onedaycat/zamus/tracer"
	"github.com/rs/zerolog/log"
)

const (
	delim = ","
)

var (
	streamList  []string
	ks          *kinesis.Kinesis
	streamNames = os.Getenv("KINESIS_STREAM_NAMES")
)

func publish(ctx context.Context, streamName string, records []*kinesis.PutRecordsRequestEntry) errors.Error {
	hctx, seg := tracer.BeginSubsegment(ctx, "publish-"+streamName)
	defer tracer.Close(seg)
	out, err := ks.PutRecordsWithContext(hctx, &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: &streamName,
	})

	if err != nil {
		appErr := errors.ErrUnablePublishKinesis.WithCaller().WithCause(err)
		tracer.AddError(seg, appErr)
		return appErr
	}

	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		appErr := errors.ErrUnablePublishKinesis.WithCaller().WithCause(errs.New("One or more events published failed"))
		tracer.AddError(seg, appErr)
		return appErr
	}

	return nil
}

func handler(ctx context.Context, stream *dynamostream.DynamoDBStreamEvent) error {
	records := stream.Records
	n := len(records)
	result := make([]*kinesis.PutRecordsRequestEntry, 0, n)

	var msg *eventstore.EventMsg
	for i := 0; i < n; i++ {
		if records[i].EventName != dynamostream.EventInsert || records[i].DynamoDB.NewImage == nil {
			continue
		}

		msg = records[i].DynamoDB.NewImage.EventMsg
		// fmt.Println("###", msg.AggregateID, msg.Seq)

		data, _ := msg.Marshal()
		result = append(result, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &msg.AggregateID,
		})
	}

	if len(result) == 0 {
		return nil
	}

	wg := errgroup.Group{}
	for _, streamName := range streamList {
		wg.Go(func() errors.Error {
			if err := publish(ctx, streamName, result); err != nil {
				return err
			}

			return nil
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

	tracer.Enable = true
	ks = kinesis.New(sess)
	tracer.AWS(ks.Client)
}

func main() {
	lambda.Start(handler)
}
