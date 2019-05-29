package source

func NewJSONHandler(handler JSONHandler) *Handler {
    return &Handler{
        source:      nil,
        jsonHandler: handler,
    }
}

func NewAPIGatewayCustomAuthorizerRequestHandler(handler APIGatewayCustomAuthorizerRequestHandler) *Handler {
    return &Handler{
        source:                                   newAPIGatewayCustomAuthorizerRequest,
        apiGatewayCustomAuthorizerRequestHandler: handler,
    }
}

func NewAPIGatewayProxyRequestHandler(handler APIGatewayProxyRequestHandler) *Handler {
    return &Handler{
        source:                        newAPIGatewayProxyRequest,
        apiGatewayProxyRequestHandler: handler,
    }
}

func NewLexEventHandler(handler LexEventHandler) *Handler {
    return &Handler{
        source:          newLexEvent,
        lexEventHandler: handler,
    }
}

func NewCloudWatchEventHandler(handler CloudWatchEventHandler) *Handler {
    return &Handler{
        source:                 newCloudWatchEvent,
        cloudWatchEventHandler: handler,
    }
}

func NewCloudwatchLogsEventHandler(handler CloudwatchLogsEventHandler) *Handler {
    return &Handler{
        source:                     newCloudwatchLogsEvent,
        cloudwatchLogsEventHandler: handler,
    }
}

func NewSQSHandler(handler SQSHandler) *Handler {
    return &Handler{
        source:     newSQS,
        sqsHandler: handler,
    }
}

func NewSNSHandler(handler SNSHandler) *Handler {
    return &Handler{
        source:     newSNS,
        snsHandler: handler,
    }
}

func NewS3EventHandler(handler S3EventHandler) *Handler {
    return &Handler{
        source:         newS3,
        s3EventHandler: handler,
    }
}

func NewKinesisHandler(handler KinesisHandler) *Handler {
    return &Handler{
        source:         newKinesis,
        kinesisHandler: handler,
    }
}

func NewFirehoseHandler(handler FirehoseHandler) *Handler {
    return &Handler{
        source:          newFirehose,
        firehoseHandler: handler,
    }
}

func NewDynamoDBStreamHandler(handler DynamoDBStreamHandler) *Handler {
    return &Handler{
        source:                newDynamoDBStream,
        dynamoDBStreamHandler: handler,
    }
}

func NewCognitoPreSignUpHandler(handler CognitoPreSignUpHandler) *Handler {
    return &Handler{
        source:                  newCognitoPreSignUp,
        cognitoPreSignUpHandler: handler,
    }
}

func NewCognitoPostConfirmHandler(handler CognitoPostConfirmHandler) *Handler {
    return &Handler{
        source:                    newCognitoPostConfirm,
        cognitoPostConfirmHandler: handler,
    }
}

func NewCognitoPreTokenHandler(handler CognitoPreTokenHandler) *Handler {
    return &Handler{
        source:                 newCognitoPreToken,
        cognitoPreTokenHandler: handler,
    }
}
