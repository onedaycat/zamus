// +build integration

package dynamodb

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/onedaycat/zamus/dql"
    "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/onedaycat/zamus/internal/common/ptr"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

var _db *dqlDynamoDB

func getDB() *dqlDynamoDB {
    if _db == nil {
        sess, err := session.NewSession(&aws.Config{
            Region:   ptr.String("ap-southeast-1"),
            Endpoint: ptr.String("http://localhost:8000"),
        })
        if err != nil {
            panic(err)
        }

        _db = New(sess, "gocqrs-eventstore-dql-dev")
        err = _db.CreateSchema(true)
        if err != nil {
            panic(err)
        }
    }

    return _db
}

func TestSaveAndGet(t *testing.T) {
    db := getDB()

    eid.FreezeID("123")

    msgs := random.EventMsgs().RandomEventMsgs(10).Build()
    msgsList := eventstore.EventMsgList{EventMsgs: msgs}
    msgsListByte, _ := common.MarshalEventMsg(&msgsList)
    appErr := errors.ErrUnableSaveDQLMessages.
        WithCaller().
        WithCause(errors.ErrUnknown).
        WithInput(map[string]interface{}{"input": 1})

    d := dql.New(db, 3, "srv1", "lamb1", "1.0.0")
    d.AddError(appErr)

    err := d.Save(context.Background(), msgsListByte)
    require.NoError(t, err)

    dqlMsg, err := db.Get(context.Background(), d.Service, "123")
    require.NoError(t, err)
    require.NotNil(t, dqlMsg)
    require.Equal(t, d.Service, dqlMsg.Service)
    require.Equal(t, "123", dqlMsg.ID)
}

func TestMultiSave(t *testing.T) {
    db := getDB()

    msgs := random.EventMsgs().RandomEventMsgs(10).Build()
    appErr := errors.ErrUnableSaveDQLMessages.
        WithCaller().
        WithCause(errors.ErrUnknown).
        WithInput(map[string]interface{}{"input": 1})

    d := dql.New(db, 3, "srv1", "lamb1", "1.0.0")
    d.AddError(appErr)

    msgList := &eventstore.EventMsgList{
        EventMsgs: msgs,
    }
    msgListByte, _ := common.MarshalEventMsg(msgList)

    err := d.Save(context.Background(), msgListByte)
    require.NoError(t, err)

    d.AddError(appErr)
    err = d.Save(context.Background(), msgListByte)
    require.NoError(t, err)
}
