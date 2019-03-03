package dql

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	"github.com/onedaycat/zamus/eventstore"
)

//go:generate mockery -name=DQL
type DQL interface {
	Save(ctx context.Context, msgs []*eventstore.EventMsg) errors.Error
	Retry() bool
	AddError(appErr errors.Error)
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

func (d *dql) Save(ctx context.Context, msgs []*eventstore.EventMsg) errors.Error {
	msgList := eventstore.EventMsgList{
		EventMsgs: msgs,
	}

	msgListByte, _ := msgList.Marshal()

	dqlMsg := &DQLMsg{
		ID:             eid.GenerateID(),
		Service:        d.Service,
		Time:           clock.Now().Unix(),
		Version:        d.Version,
		LambdaFunction: d.LambdaFunction,
		EventMsgs:      msgListByte,
		Errors:         d.Errors,
	}

	if err := d.Storage.Save(ctx, dqlMsg); err != nil {
		return err
	}

	d.Remain = d.MaxRetry
	d.Errors = nil

	return nil
}

func (d *dql) Retry() bool {
	d.Remain--
	if d.Remain <= 0 {
		return false
	}

	return true
}

func (d *dql) AddError(appErr errors.Error) {
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
