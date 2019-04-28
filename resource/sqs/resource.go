package sqs

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/onedaycat/zamus/internal/common/ptr"
)

var (
	protocal = "sqs"
	trueStr  = "true"
)

type CreateSQSSubSNSInput struct {
	SNSTopicArn       string
	QueueName         string
	FilterEvents      []string
	VisibilityTimeout string
}

type CreateSQSSubSNSOutput struct {
	QueueURL     string
	QueueARN     string
	SubscribeArn string
}

type m map[string]interface{}
type a []m

func CreateSQSSubSNS(snsClient *sns.SNS, sqsClient *sqs.SQS, input *CreateSQSSubSNSInput) (*CreateSQSSubSNSOutput, error) {
	output := &CreateSQSSubSNSOutput{}

	if input.VisibilityTimeout == "" {
		input.VisibilityTimeout = "60"
	}

	outCreateQueue, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: &input.QueueName,
	})

	if err != nil {
		return nil, err
	}

	output.QueueURL = *outCreateQueue.QueueUrl
	output.QueueARN = getQueueArnFromURL(output.QueueURL)

	policy := m{
		"Version": "2012-10-17",
		"Statement": a{
			{
				"Sid":       "allow-sns-messages",
				"Effect":    "Allow",
				"Principal": "*",
				"Resource":  "*",
				"Action":    "SQS:SendMessage",
			},
		},
	}

	b, _ := json.Marshal(policy)
	bstr := string(b)

	_, err = sqsClient.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueUrl: &output.QueueURL,
		Attributes: map[string]*string{
			sqs.QueueAttributeNamePolicy:            &bstr,
			sqs.QueueAttributeNameVisibilityTimeout: &input.VisibilityTimeout,
		},
	})

	if err != nil {
		Delete(snsClient, sqsClient, output.QueueURL, "")
		return nil, err
	}

	outSub, err := snsClient.Subscribe(&sns.SubscribeInput{
		TopicArn: &input.SNSTopicArn,
		Protocol: &protocal,
		Endpoint: &output.QueueARN,
		Attributes: map[string]*string{
			"RawMessageDelivery": &trueStr,
			"FilterPolicy":       createFilterEvent(input.FilterEvents),
		},
	})

	if err != nil {
		Delete(snsClient, sqsClient, output.QueueURL, "")
		return nil, err
	}

	output.SubscribeArn = *outSub.SubscriptionArn

	return output, nil
}

func Delete(snsClient *sns.SNS, sqsClient *sqs.SQS, queueUrl, subArn string) {
	if queueUrl != "" {
		_, _ = sqsClient.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: &queueUrl,
		})
	}

	if subArn != "" {
		_, _ = snsClient.Unsubscribe(&sns.UnsubscribeInput{
			SubscriptionArn: &subArn,
		})
	}
}

func createFilterEvent(events []string) *string {
	if len(events) == 0 {
		return ptr.String("{}")
	}

	filter := m{
		"event": events,
	}

	data, _ := json.Marshal(filter)

	return ptr.String(string(data))
}

func getQueueArnFromURL(url string) string {
	sp := strings.Split(url, "/")
	sp[2] = strings.TrimPrefix(sp[2], "sqs.")
	sp[2] = strings.TrimSuffix(sp[2], ".amazonaws.com")
	queueArn := "arn:aws:sqs:" + sp[2] + ":" + sp[3] + ":" + sp[4]

	return queueArn
}
