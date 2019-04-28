package sqs

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	snsRes "github.com/onedaycat/zamus/resource/sns"
	"github.com/stretchr/testify/require"
)

func SkipTestResource(t *testing.T) {
	sess := session.Must(session.NewSession())
	snsClient := sns.New(sess)
	sqsClient := sqs.New(sess)

	snsOutput, err := snsRes.Create(snsClient, &snsRes.CreateSNSInput{
		Topic: "test",
	})
	require.NoError(t, err)
	defer snsRes.Delete(snsClient, snsOutput.TopicArn)

	sqsOutput, err := CreateSQSSubSNS(snsClient, sqsClient, &CreateSQSSubSNSInput{
		QueueName:    "test",
		SNSTopicArn:  snsOutput.TopicArn,
		FilterEvents: []string{"event1", "event2"},
	})
	require.NoError(t, err)
	defer Delete(snsClient, sqsClient, sqsOutput.QueueURL, sqsOutput.SubscribeArn)
	require.NotNil(t, sqsOutput)
}
