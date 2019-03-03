package dql

import (
	"context"
	"strconv"

	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/eventstore"
)

const (
	colon = ":"
)

//go:generate mockery -name=DQL
type DQL interface {
	Save(ctx context.Context) error
	Retry() bool
	AddEventMsgError(msg *eventstore.EventMsg, errStack []string)
}

type dql struct {
	Storage        Storage
	MaxRetry       int
	Remain         int
	DQLMsgs        DQLMsgs
	Service        string
	LambdaFunction string
	Version        string
}

func New(storage Storage, maxRetry int, service, lambdaFunc, version string) *dql {
	return &dql{storage, maxRetry, maxRetry, make(DQLMsgs, 0, 100), service, lambdaFunc, version}
}

func (d *dql) Save(ctx context.Context) error {
	if err := d.Storage.MultiSave(ctx, d.DQLMsgs); err != nil {
		return err
	}

	d.Remain = d.MaxRetry
	d.DQLMsgs = make(DQLMsgs, 0, 100)

	return nil
}

func (d *dql) Retry() bool {
	d.Remain--
	if d.Remain == 0 {
		return false
	}

	return true
}

func (d *dql) AddEventMsgError(msg *eventstore.EventMsg, errStack []string) {
	event, _ := msg.Marshal()
	now := clock.Now().Unix()

	dqlMsg := &DQLMsg{
		ID:             d.Service + colon + strconv.FormatInt(now, 10) + colon + msg.AggregateID + colon + strconv.FormatInt(msg.Seq, 10),
		Service:        d.Service,
		Version:        d.Version,
		LambdaFunction: d.LambdaFunction,
		EventType:      msg.EventType,
		AggregateID:    msg.AggregateID,
		EventID:        msg.EventID,
		Seq:            msg.Seq,
		Time:           msg.Time,
		DQLTime:        now,
		EventMsg:       event,
		Error:          errStack,
	}

	d.DQLMsgs = append(d.DQLMsgs, dqlMsg)
}
