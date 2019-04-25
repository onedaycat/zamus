package main

import (
    "os"

    "github.com/aws/aws-sdk-go/aws/session"
    dlqService "github.com/onedaycat/zamus/dlq/service"
    dlqDynamoDB "github.com/onedaycat/zamus/dlq/storage/dynamodb"
    "github.com/onedaycat/zamus/service"
)

func main() {

    config := &service.Config{
        AppStage:    os.Getenv("APP_STAGE"),
        Service:     os.Getenv("APP_SERVICE"),
        Version:     os.Getenv("APP_VERSION"),
        SentryDSN:   os.Getenv("APP_SENTRY_DSN"),
        EnableTrace: false,
    }

    sess := session.Must(session.NewSession())

    storage := dlqDynamoDB.New(sess, os.Getenv("APP_TABLE"))

    dh := dlqService.NewHandler(storage)
    h := service.NewHandler(config)

    h.RegisterHandler(dlqService.SaveDLQMsgMethod, dh.SaveDLQMsg)

    h.StartLambda()
}
