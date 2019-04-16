package main

import (
    "context"
    "strconv"
    "testing"
    "time"

    "github.com/aws/aws-lambda-go/events"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

func TestHandle(t *testing.T) {
    var data []byte
    var err error
    now := time.Now()
    msgs := random.EventMsgs().RandomEventMsgs(10, random.WithMetadata(eventstore.NewMetadata().SetUserID("1"))).Build()

    event := &events.KinesisFirehoseEvent{
        Records: make([]events.KinesisFirehoseEventRecord, 10),
    }

    for i, msg := range msgs {
        data, err = eventstore.MarshalEventMsg(msg)
        require.NoError(t, err)
        event.Records[i].Data = data
        event.Records[i].RecordID = strconv.Itoa(i)
        event.Records[i].ApproximateArrivalTimestamp = events.MilliSecondsEpochTime{Time: now}
    }

    data, err = common.MarshalJSON(event)
    require.NoError(t, err)

    h := &Handler{}
    result, err := h.Invoke(context.Background(), data)

    res := &events.KinesisFirehoseResponse{}
    err = common.UnmarshalJSON(result, res)
    require.NoError(t, err)

    require.Len(t, res.Records, 10)

    var msg *eventstore.EventMsg
    for i, rec := range res.Records {
        require.Equal(t, events.KinesisFirehoseTransformedStateOk, rec.Result)
        require.Equal(t, strconv.Itoa(i), rec.RecordID)

        msg = &eventstore.EventMsg{}
        err = common.UnmarshalJSON(rec.Data, msg)
        require.NoError(t, err)
        require.Equal(t, msgs[i], msg)
    }
}
