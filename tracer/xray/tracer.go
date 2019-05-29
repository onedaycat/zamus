package xray

import (
    "context"

    "github.com/aws/aws-sdk-go/aws/client"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-xray-sdk-go/xray"
    "github.com/onedaycat/zamus/lambda"
)

var Tracer *XRay

type XRay struct {
    global *xray.Segment
    seg    *xray.Segment
}

func init() {
    Tracer = New()
}

func New() *XRay {
    return &XRay{}
}

func (x *XRay) InitGlobalSpan(ctx context.Context) {
    x.global = xray.GetSegment(ctx)
}

func (x *XRay) WrapLambda(handler lambda.Handle) lambda.Handler {
    return handler.Invoke
}

func (x *XRay) WrapAWSSession(sess *session.Session) {
}

func (x *XRay) WrapAWSClient(awsClient *client.Client) {
    xray.AWS(awsClient)
}

func (x *XRay) SetTag(ctx context.Context, key string, value interface{}) {
    if x.global == nil {
        return
    }

    _ = x.global.AddMetadata(key, value)
}
