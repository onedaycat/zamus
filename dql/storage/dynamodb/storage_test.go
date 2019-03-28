// +build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/onedaycat/zamus/common/ptr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/stretchr/testify/require"
)

var _db *dqlDynamoDB

func getDB() *dqlDynamoDB {
	if _db == nil {
		sess, err := session.NewSession(&aws.Config{
			Credentials: credentials.NewEnvCredentials(),
			Region:      ptr.String("ap-southeast-1"),
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

func TestJSONSave(t *testing.T) {
	db := getDB()

	msgsByte := random.EventMsgs().RandomEventMsgs(10).BuildJSON()
	appErr := errors.ErrUnbleSaveDQLMessages.
		WithCaller().
		WithCause(errors.ErrUnknown).
		WithInput(map[string]interface{}{"input": 1})

	d := dql.New(db, 3, "srv1", "lamb1", "1.0.0")
	d.AddError(appErr)

	err := d.Save(context.Background(), msgsByte)
	require.NoError(t, err)
}

func TestMultiSave(t *testing.T) {
	db := getDB()

	msgs := random.EventMsgs().RandomEventMsgs(10).Build()
	appErr := errors.ErrUnbleSaveDQLMessages.
		WithCaller().
		WithCause(errors.ErrUnknown).
		WithInput(map[string]interface{}{"input": 1})

	d := dql.New(db, 3, "srv1", "lamb1", "1.0.0")
	d.AddError(appErr)

	msgList := eventstore.EventMsgList{
		EventMsgs: msgs,
	}
	msgListByte, _ := msgList.Marshal()

	err := d.Save(context.Background(), msgListByte)
	require.NoError(t, err)

	d.AddError(appErr)
	err = d.Save(context.Background(), msgListByte)
	require.NoError(t, err)
}
