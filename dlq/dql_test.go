package dlq_test

import (
    "context"
    "testing"
    "time"

    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/dlq/mocks"
    "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common/clock"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

func TestDLQ(t *testing.T) {
    storage := &mocks.Storage{}
    appErr := errors.ErrEncodingNotSupported.
        WithCaller().
        WithCause(errors.ErrUnknown).
        WithInput(map[string]interface{}{"input": "1"})

    d := dlq.New(storage, &dlq.Config{
        Service:    "srv1",
        SourceType: dlq.Lambda,
        Source:     "fn1",
        Version:    "1.0.0",
        MaxRetry:   3,
    })

    msgs := random.EventMsgs().RandomEventMsgs(10).Build()
    msgList := &event.MsgList{Msgs: msgs}

    ok := d.Retry()
    require.True(t, ok)
    require.Equal(t, 2, d.Remain)
    d.AddError(appErr)

    ok = d.Retry()
    require.True(t, ok)
    require.Equal(t, 1, d.Remain)
    d.AddError(appErr)

    ok = d.Retry()
    require.False(t, ok)
    require.Equal(t, 0, d.Remain)
    d.AddError(appErr)

    now := time.Now()
    eid.FreezeID("1")
    clock.Freeze(now)

    msgListByte, _ := event.MarshalMsg(msgList)

    dlqMsg := &dlq.DLQMsg{
        ID:         "1",
        Service:    "srv1",
        Time:       now.Unix(),
        Version:    "1.0.0",
        SourceType: dlq.Lambda,
        Source:     "fn1",
        EventMsgs:  msgListByte,
        Errors:     d.Errors,
    }

    ctx := context.Background()
    storage.On("Save", ctx, dlqMsg).Return(nil)

    err := d.Save(ctx, msgListByte)

    require.NoError(t, err)
    require.Equal(t, 3, d.Remain)
    require.Nil(t, d.Errors)
}

func TestDLQOnlyOne(t *testing.T) {
    storage := &mocks.Storage{}

    d := dlq.New(storage, &dlq.Config{
        Service:    "srv1",
        SourceType: dlq.Lambda,
        Source:     "fn1",
        Version:    "1.0.0",
        MaxRetry:   1,
    })

    ok := d.Retry()
    require.False(t, ok)
    require.Equal(t, 0, d.Remain)
}

func TestDLQZero(t *testing.T) {
    storage := &mocks.Storage{}

    d := dlq.New(storage, &dlq.Config{
        Service:    "srv1",
        SourceType: dlq.Lambda,
        Source:     "fn1",
        Version:    "1.0.0",
        MaxRetry:   0,
    })

    ok := d.Retry()
    require.False(t, ok)
    require.Equal(t, -1, d.Remain)
}
