package dql

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
)

//go:generate mockery -name=DQL
type DQL interface {
	Save(ctx context.Context, data []byte) errors.Error
	Retry() bool
	AddError(appErr errors.Error)
	GetDQLErrors() DQLErrors
}

type dql struct {
	Storage        Storage
	MaxRetry       int
	Remain         int
	Errors         DQLErrors
	Service        string
	LambdaFunction string
	Version        string
}

func New(storage Storage, maxRetry int, service, lambdaFunc, version string) *dql {
	return &dql{storage, maxRetry, maxRetry, nil, service, lambdaFunc, version}
}

func (d *dql) Save(ctx context.Context, data []byte) errors.Error {
	dqlMsg := &DQLMsg{
		ID:             eid.GenerateID(),
		Service:        d.Service,
		Time:           clock.Now().Unix(),
		Version:        d.Version,
		LambdaFunction: d.LambdaFunction,
		EventMsgs:      data,
		Errors:         d.Errors,
	}

	if err := d.Storage.Save(ctx, dqlMsg); err != nil {
		return err
	}

	d.Remain = d.MaxRetry
	d.Errors = nil

	return nil
}

func (d *dql) Reset() {
	d.Remain = d.MaxRetry
	d.Errors = d.Errors[:0]
}

func (d *dql) ResetWithRetry(maxRetry int) {
	d.MaxRetry = maxRetry
	d.Remain = d.MaxRetry
	d.Errors = d.Errors[:0]
}

func (d *dql) Retry() bool {
	d.Remain--
	if d.Remain <= 0 {
		return false
	}

	return true
}

func (d *dql) AddError(appErr errors.Error) {
	if appErr == nil {
		return
	}

	if d.Errors == nil {
		d.Errors = make(DQLErrors, 0, 10)
	}

	dqlErr := &DQLError{
		Message: appErr.Error(),
		Stacks:  appErr.StackStrings(),
	}

	if appErr.GetCause() != nil {
		dqlErr.Cause = appErr.GetCause().Error()
	}

	if appErr.GetInput() != nil {
		dqlErr.Input = appErr.GetInput()
	}

	d.Errors = append(d.Errors, dqlErr)
}

func (d *dql) GetDQLErrors() DQLErrors {
	return d.Errors
}
