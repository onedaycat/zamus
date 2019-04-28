package sns

import (
	"github.com/aws/aws-sdk-go/service/sns"
)

type CreateSNSInput struct {
	Topic string
}

type CreateSNSOutput struct {
	TopicArn string
}

func Create(snsClient *sns.SNS, input *CreateSNSInput) (*CreateSNSOutput, error) {
	output := &CreateSNSOutput{}
	outCreateSNS, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: &input.Topic,
	})

	if err != nil {
		return nil, err
	}

	output.TopicArn = *outCreateSNS.TopicArn

	return output, nil
}

func Delete(snsClient *sns.SNS, topicArn string) {
	_, _ = snsClient.DeleteTopic(&sns.DeleteTopicInput{
		TopicArn: &topicArn,
	})
}
