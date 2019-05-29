package source

import (
    "context"
    "encoding/json"

    "github.com/aws/aws-lambda-go/events"
    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
)

var jsonen = jsoniter.ConfigCompatibleWithStandardLibrary

type JSONHandler func(ctx context.Context, src json.RawMessage) (interface{}, error)
type APIGatewayCustomAuthorizerRequestHandler func(ctx context.Context, src *events.APIGatewayCustomAuthorizerRequest) (*events.APIGatewayCustomAuthorizerResponse, error)
type APIGatewayProxyRequestHandler func(ctx context.Context, src *events.APIGatewayProxyRequest) (*events.APIGatewayProxyResponse, error)
type LexEventHandler func(ctx context.Context, src *events.LexEvent) (interface{}, error)
type CloudWatchEventHandler func(ctx context.Context, src *events.CloudWatchEvent) (interface{}, error)
type CloudwatchLogsEventHandler func(ctx context.Context, src *events.CloudwatchLogsEvent) (interface{}, error)
type SQSHandler func(ctx context.Context, src *events.SQSEvent) (interface{}, error)
type SNSHandler func(ctx context.Context, src *events.SNSEvent) (interface{}, error)
type S3EventHandler func(ctx context.Context, src *events.S3Event) (interface{}, error)
type KinesisHandler func(ctx context.Context, src *events.KinesisEvent) (interface{}, error)
type FirehoseHandler func(ctx context.Context, src *events.KinesisFirehoseEvent) (interface{}, error)
type DynamoDBStreamHandler func(ctx context.Context, src *events.DynamoDBEvent) (interface{}, error)
type CognitoPreSignUpHandler func(ctx context.Context, src *events.CognitoEventUserPoolsPreSignup) *events.CognitoEventUserPoolsPreSignup
type CognitoPostConfirmHandler func(ctx context.Context, src *events.CognitoEventUserPoolsPostConfirmation) *events.CognitoEventUserPoolsPostConfirmation
type CognitoPreTokenHandler func(ctx context.Context, src *events.CognitoEventUserPoolsPreTokenGen) *events.CognitoEventUserPoolsPreTokenGen

type newSrouce func() interface{}

//noinspection GoNameStartsWithPackageName
type Handler struct {
    source                                   newSrouce
    jsonHandler                              JSONHandler
    apiGatewayCustomAuthorizerRequestHandler APIGatewayCustomAuthorizerRequestHandler
    apiGatewayProxyRequestHandler            APIGatewayProxyRequestHandler
    lexEventHandler                          LexEventHandler
    cloudWatchEventHandler                   CloudWatchEventHandler
    cloudwatchLogsEventHandler               CloudwatchLogsEventHandler
    sqsHandler                               SQSHandler
    snsHandler                               SNSHandler
    s3EventHandler                           S3EventHandler
    kinesisHandler                           KinesisHandler
    firehoseHandler                          FirehoseHandler
    dynamoDBStreamHandler                    DynamoDBStreamHandler
    cognitoPreSignUpHandler                  CognitoPreSignUpHandler
    cognitoPostConfirmHandler                CognitoPostConfirmHandler
    cognitoPreTokenHandler                   CognitoPreTokenHandler
}

func (h *Handler) ParseSource(ctx context.Context, payload json.RawMessage) interface{} {
    if h.source == nil {
        return payload
    }

    source := h.source()
    err := jsonen.Unmarshal(payload, source)
    if err != nil {
        panic(errors.InternalError("UnableParseSource", "UnableParseSource: "+err.Error()))
    }

    return source
}

func (h *Handler) ParseSources(ctx context.Context, payload json.RawMessage) interface{} {
    panic(errors.InternalError("BatchInvokeNotAllowed", "Batch invoke not allowed"))
}

func (h *Handler) BatchHandler(ctx context.Context, sources interface{}) (interface{}, error) {
    panic(errors.InternalError("BatchInvokeNotAllowed", "Batch invoke not allowed"))
}

func (h *Handler) Handler(ctx context.Context, source interface{}) (interface{}, error) {
    var result interface{}
    var err error

    switch src := source.(type) {
    case json.RawMessage:
        result, err = h.jsonHandler(ctx, src)
    case *events.APIGatewayCustomAuthorizerRequest:
        result, err = h.apiGatewayCustomAuthorizerRequestHandler(ctx, src)
    case *events.APIGatewayProxyRequest:
        result, err = h.apiGatewayProxyRequestHandler(ctx, src)
    case *events.LexEvent:
        result, err = h.lexEventHandler(ctx, src)
    case *events.CloudWatchEvent:
        result, err = h.cloudWatchEventHandler(ctx, src)
    case *events.CloudwatchLogsEvent:
        result, err = h.cloudwatchLogsEventHandler(ctx, src)
    case *events.SQSEvent:
        result, err = h.sqsHandler(ctx, src)
    case *events.SNSEvent:
        result, err = h.snsHandler(ctx, src)
    case *events.S3Event:
        result, err = h.s3EventHandler(ctx, src)
    case *events.KinesisEvent:
        result, err = h.kinesisHandler(ctx, src)
    case *events.KinesisFirehoseEvent:
        result, err = h.firehoseHandler(ctx, src)
    case *events.DynamoDBEvent:
        result, err = h.dynamoDBStreamHandler(ctx, src)
    case *events.CognitoEventUserPoolsPreSignup:
        result = h.cognitoPreSignUpHandler(ctx, src)
    case *events.CognitoEventUserPoolsPostConfirmation:
        result = h.cognitoPostConfirmHandler(ctx, src)
    case *events.CognitoEventUserPoolsPreTokenGen:
        result = h.cognitoPreTokenHandler(ctx, src)
    }

    return result, err
}
