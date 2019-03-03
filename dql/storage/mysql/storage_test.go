// +build integration

package mysql

import (
	"context"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/require"
)

var db *dqlMySQL

func getDB() *dqlMySQL {
	if db == nil {
		conn, err := sqlx.Connect("mysql", os.Getenv("ZAMUS_DQL_DATASOURCE"))
		if err != nil {
			panic(err)
		}
		db = New(conn)
	}

	return db
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
