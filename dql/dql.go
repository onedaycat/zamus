package dql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/eventstore"
)

//go:generate mockery -name=DQL
type DQL interface {
	Save(ctx context.Context) error
	IncreateRetry() bool
	AddEventMsgError(msg *eventstore.EventMsg, errStack []string)
}

type dql struct {
	storage        Storage
	retry          int
	nRetry         int
	msgs           DQLMsgs
	service        string
	lambdaFunction string
	version        string
}

func New(storage Storage, retry int, service, lambdaFunc, version string) DQL {
	return &dql{storage, retry, 0, make(DQLMsgs, 0, 100), service, lambdaFunc, version}
}

func (d *dql) Save(ctx context.Context) error {
	if err := d.storage.MultiSave(ctx, d.msgs); err != nil {
		return err
	}

	d.nRetry = 0
	d.msgs = make(DQLMsgs, 0, 100)

	return nil
}

func (d *dql) IncreateRetry() bool {
	d.retry++
	if d.retry == d.nRetry {
		return true
	}

	return false
}

func (d *dql) AddEventMsgError(msg *eventstore.EventMsg, errStack []string) {
	event, _ := msg.Marshal()
	eventByte := make([]byte, base64.StdEncoding.EncodedLen(len(event)))
	base64.StdEncoding.Encode(eventByte, event)

	errStackJSON, _ := json.Marshal(errStack)

	now := clock.Now().Unix()

	dqlMsg := &DQLMsg{
		ID:             d.service + ":" + msg.AggregateID + ":" + strconv.FormatInt(msg.Seq, 10) + ":" + strconv.FormatInt(now, 10),
		Service:        d.service,
		Version:        d.version,
		LambdaFunction: d.lambdaFunction,
		EventType:      msg.EventType,
		AggregateID:    msg.AggregateID,
		EventID:        msg.EventID,
		Seq:            msg.Seq,
		Time:           msg.Time,
		DQLTime:        now,
		EventMsg:       eventByte,
		Error:          errStackJSON,
	}

	d.msgs = append(d.msgs, dqlMsg)
}
