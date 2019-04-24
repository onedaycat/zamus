package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/random"
	"github.com/stretchr/testify/require"
)

func TestHandle(t *testing.T) {
	var data []byte
	var err error
	now := time.Now()
	msgs := random.EventMsgs().RandomEventMsgs(10, random.WithMetadata(event.Metadata{"u": "1"})).Build()

	evt := &events.KinesisFirehoseEvent{
		Records: make([]events.KinesisFirehoseEventRecord, 10),
	}

	for i, msg := range msgs {
		data, err = event.MarshalMsg(msg)
		require.NoError(t, err)
		evt.Records[i].Data = data
		evt.Records[i].RecordID = strconv.Itoa(i)
		evt.Records[i].ApproximateArrivalTimestamp = events.MilliSecondsEpochTime{Time: now}
	}

	data, err = common.MarshalJSON(evt)
	require.NoError(t, err)

	h := &Handler{}
	result, err := h.Invoke(context.Background(), data)
	require.NoError(t, err)

	res := &events.KinesisFirehoseResponse{}
	err = common.UnmarshalJSON(result, res)
	require.NoError(t, err)

	require.Len(t, res.Records, 10)

	var msg *event.Msg
	for i, rec := range res.Records {
		require.Equal(t, events.KinesisFirehoseTransformedStateOk, rec.Result)
		require.Equal(t, strconv.Itoa(i), rec.RecordID)

		msg = &event.Msg{}
		err = common.UnmarshalJSON(rec.Data, msg)
		require.NoError(t, err)
		require.Equal(t, msgs[i], msg)
	}
}
