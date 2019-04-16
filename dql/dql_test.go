package dql_test

import (
    "context"
    "testing"
    "time"

    "github.com/onedaycat/zamus/dql"
    "github.com/onedaycat/zamus/dql/mocks"
    "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/internal/common/clock"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

func TestDQL(t *testing.T) {
    storage := &mocks.Storage{}
    appErr := errors.ErrEncodingNotSupported.
        WithCaller().
        WithCause(errors.ErrUnknown).
        WithInput(map[string]interface{}{"input": "1"})

    d := dql.New(storage, 3, "srv1", "fn1", "1.0.0")

    msgs := random.EventMsgs().RandomEventMsgs(10).Build()
    msgList := &eventstore.EventMsgList{EventMsgs: msgs}

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

    msgListByte, _ := eventstore.MarshalEventMsg(msgList)

    dqlMsg := &dql.DQLMsg{
        ID:             "1",
        Service:        "srv1",
        Time:           now.Unix(),
        Version:        "1.0.0",
        LambdaFunction: "fn1",
        EventMsgs:      msgListByte,
        Errors:         d.Errors,
    }

    ctx := context.Background()
    storage.On("Save", ctx, dqlMsg).Return(nil)

    err := d.Save(ctx, msgListByte)

    require.NoError(t, err)
    require.Equal(t, 3, d.Remain)
    require.Nil(t, d.Errors)
}

func TestDQLOnlyOne(t *testing.T) {
    storage := &mocks.Storage{}

    d := dql.New(storage, 1, "srv1", "fn1", "1.0.0")

    ok := d.Retry()
    require.False(t, ok)
    require.Equal(t, 0, d.Remain)
}

func TestDQLZero(t *testing.T) {
    storage := &mocks.Storage{}

    d := dql.New(storage, 0, "srv1", "fn1", "1.0.0")

    ok := d.Retry()
    require.False(t, ok)
    require.Equal(t, -1, d.Remain)
}
