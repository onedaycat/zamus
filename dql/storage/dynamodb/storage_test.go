// +build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/require"
)

var _db *dqlDynamoDB

func getDB() *dqlDynamoDB {
	if _db == nil {
		sess, err := session.NewSession(&aws.Config{
			Credentials: credentials.NewEnvCredentials(),
			Region:      aws.String("ap-southeast-1"),
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

func TestMultiSave(t *testing.T) {
	db := getDB()

	eventsRand := random.EventMsgs()
	for i := 0; i < 10; i++ {
		eventsRand.Add("et1", map[string]interface{}{"id": "1"})
	}

	events := eventsRand.Build()
	errStack := errors.ErrUnbleSaveDQLMessages.WithCaller().PrintStack()

	d := dql.New(db, 3, "srv1", "lamb1", "1.0.0")
	for _, event := range events {
		d.AddEventMsgError(event, errStack)
	}

	err := d.Save(context.Background())
	require.NoError(t, err)

	for _, event := range events {
		d.AddEventMsgError(event, errStack)
	}
	err = d.Save(context.Background())
	require.NoError(t, err)
}
