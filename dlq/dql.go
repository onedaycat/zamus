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

type Config struct {
    Service    string
    MaxRetry   int
    LambdaType LambdaType
    Fn         string
    Version    string
}

type dlq struct {
    Storage  Storage
    config   *Config
    Errors   DLQErrors
    Remain   int
    MaxRetry int
}

func New(storage Storage, config *Config) *dlq {
    return &dlq{storage, config, nil, config.MaxRetry, config.MaxRetry}
}

func (d *dlq) Save(ctx context.Context, data []byte) errors.Error {
    dlqMsg := &DLQMsg{
        ID:         eid.GenerateID(),
        Service:    d.config.Service,
        Time:       clock.Now().Unix(),
        Version:    d.config.Version,
        Data:       data,
        Errors:     d.Errors,
        LambdaType: d.config.LambdaType,
        Fn:         d.config.Fn,
    }

    if err := d.Storage.Save(ctx, dlqMsg); err != nil {
        return err
    }

    d.Remain = d.config.MaxRetry
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
