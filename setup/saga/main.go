package main

import (
    "os"

    "github.com/aws/aws-sdk-go/aws/session"
    sagaService "github.com/onedaycat/zamus/saga/service"
    sagaDynamoDB "github.com/onedaycat/zamus/saga/storage/dynamodb"
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

    storage := sagaDynamoDB.New(sess, os.Getenv("APP_TABLE"))

    dh := sagaService.NewHandler(storage)
    h := service.NewHandler(config)

    h.RegisterHandler(sagaService.SaveStateMethod, dh.SaveState)
    h.RegisterHandler(sagaService.GetStateMethod, dh.GetState)

    h.StartLambda()
}
