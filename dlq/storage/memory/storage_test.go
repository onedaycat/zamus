// +build integration

package memory

import (
    "context"
    "testing"

    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

var _db *dlqMemory

func getDB() *dlqMemory {
    if _db == nil {
        _db = New()
    }

    return _db
}

func TestSaveAndGet(t *testing.T) {
    db := getDB()

    eid.FreezeID("123")

    msgs := random.EventMsgs().RandomEventMsgs(10).Build()
    msgsList := event.MsgList{Msgs: msgs}
    msgsListByte, _ := event.MarshalMsg(&msgsList)
    appErr := errors.ErrUnableSaveDLQMessages.
        WithCaller().
        WithCause(errors.ErrUnknown).
        WithInput(map[string]interface{}{"input": 1})

    d := dlq.New(db, &dlq.Config{
        Service:    "srv1",
        Fn:         "lamb1",
        LambdaType: dlq.Reactor,
        MaxRetry:   3,
        Version:    "1.0.0",
    })
    d.AddError(appErr)

    err := d.Save(context.Background(), msgsListByte)
    require.NoError(t, err)

    dlqMsg, err := db.Get(context.Background(), dlq.Reactor, "123")
    require.NoError(t, err)
    require.NotNil(t, dlqMsg)
    require.Equal(t, "srv1", dlqMsg.Service)
    require.Equal(t, "123", dlqMsg.ID)
}

func TestMultiSave(t *testing.T) {
    db := getDB()

    msgs := random.EventMsgs().RandomEventMsgs(10).Build()
    appErr := errors.ErrUnableSaveDLQMessages.
        WithCaller().
        WithCause(errors.ErrUnknown).
        WithInput(map[string]interface{}{"input": 1})

    d := dlq.New(db, &dlq.Config{
        Service:    "srv1",
        Fn:         "lamb1",
        LambdaType: dlq.Reactor,
        MaxRetry:   3,
        Version:    "1.0.0",
    })
    d.AddError(appErr)

    msgList := &event.MsgList{
        Msgs: msgs,
    }
    msgListByte, _ := event.MarshalMsg(msgList)

    err := d.Save(context.Background(), msgListByte)
    require.NoError(t, err)

    d.AddError(appErr)
    err = d.Save(context.Background(), msgListByte)
    require.NoError(t, err)
}
