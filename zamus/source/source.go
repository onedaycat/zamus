package source

import (
    "github.com/aws/aws-lambda-go/events"
)

func newAPIGatewayCustomAuthorizerRequest() interface{} {
    return &events.APIGatewayCustomAuthorizerRequest{}
}

func newAPIGatewayProxyRequest() interface{} {
    return &events.APIGatewayProxyRequest{}
}

func newLexEvent() interface{} {
    return &events.LexEvent{}
}

func newCloudWatchEvent() interface{} {
    return &events.CloudWatchEvent{}
}

func newCloudwatchLogsEvent() interface{} {
    return &events.CloudwatchLogsEvent{}
}

func newSQS() interface{} {
    return &events.SQSEvent{}
}

func newSNS() interface{} {
    return &events.SNSEvent{}
}

func newKinesis() interface{} {
    return &events.KinesisEvent{}
}

func newFirehose() interface{} {
    return &events.KinesisFirehoseEvent{}
}

func newDynamoDBStream() interface{} {
    return &events.DynamoDBEvent{}
}

func newS3() interface{} {
    return &events.S3Event{}
}

func newCognitoPreSignUp() interface{} {
    return &events.CognitoEventUserPoolsPreSignup{}
}

func newCognitoPostConfirm() interface{} {
    return &events.CognitoEventUserPoolsPostConfirmation{}
}

func newCognitoPreToken() interface{} {
    return &events.CognitoEventUserPoolsPreTokenGen{}
}
