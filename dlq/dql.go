package dlq

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/internal/common/clock"
    "github.com/onedaycat/zamus/internal/common/eid"
)

//go:generate mockery -name=DLQ
type DLQ interface {
    Save(ctx context.Context, data []byte) errors.Error
    Retry() bool
    AddError(appErr errors.Error)
    GetDLQErrors() DLQErrors
}

type dlq struct {
    Storage        Storage
    MaxRetry       int
    Remain         int
    Errors         DLQErrors
    Service        string
    LambdaFunction string
    Version        string
}

func New(storage Storage, maxRetry int, service, lambdaFunc, version string) *dlq {
    return &dlq{storage, maxRetry, maxRetry, nil, service, lambdaFunc, version}
}

func (d *dlq) Save(ctx context.Context, data []byte) errors.Error {
    dlqMsg := &DLQMsg{
        ID:             eid.GenerateID(),
        Service:        d.Service,
        Time:           clock.Now().Unix(),
        Version:        d.Version,
        LambdaFunction: d.LambdaFunction,
        EventMsgs:      data,
        Errors:         d.Errors,
    }

    if err := d.Storage.Save(ctx, dlqMsg); err != nil {
        return err
    }

    d.Remain = d.MaxRetry
    d.Errors = nil

    return nil
}

func (d *dlq) Reset() {
    d.Remain = d.MaxRetry
    d.Errors = d.Errors[:0]
}

func (d *dlq) ResetWithRetry(maxRetry int) {
    d.MaxRetry = maxRetry
    d.Remain = d.MaxRetry
    d.Errors = d.Errors[:0]
}

func (d *dlq) Retry() bool {
    d.Remain--
    if d.Remain <= 0 {
        return false
    }

    return true
}

func (d *dlq) AddError(appErr errors.Error) {
    if appErr == nil {
        return
    }

    if d.Errors == nil {
        d.Errors = make(DLQErrors, 0, 10)
    }

    dlqErr := &DLQError{
        Message: appErr.Error(),
        Stacks:  appErr.StackStrings(),
    }

    if appErr.GetCause() != nil {
        dlqErr.Cause = appErr.GetCause().Error()
    }

    if appErr.GetInput() != nil {
        dlqErr.Input = appErr.GetInput()
    }

    d.Errors = append(d.Errors, dlqErr)
}

func (d *dlq) GetDLQErrors() DLQErrors {
    return d.Errors
}
