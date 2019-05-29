package tracer

import (
    "context"

    "github.com/aws/aws-sdk-go/aws/client"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/onedaycat/zamus/lambda"
)

type Tracer interface {
    InitGlobalSpan(ctx context.Context)
    WrapLambda(handler lambda.Handle) lambda.Handler
    WrapAWSSession(sess *session.Session)
    WrapAWSClient(awsClient *client.Client)
    SetTag(ctx context.Context, key string, value interface{})
}
