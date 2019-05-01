// +build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/onedaycat/zamus/internal/common/ptr"
	"github.com/onedaycat/zamus/random"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *EventStoreStorage

func getDB() *EventStoreStorage {
	if _db == nil {
		sess, err := session.NewSession(&aws.Config{
			Endpoint: ptr.String("http://localhost:8000"),
			Region:   ptr.String("xxx"),
		})
		if err != nil {
			panic(err)
		}

		_db = New(dynamodb.New(sess), "eventstore")
		err = _db.CreateSchema()
		if err != nil {
			panic(err)
		}
	}

	return _db
}

func TestSave(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		msgs := random.EventMsgs().
			Add(random.WithEvent(&domain.StockItemCreated{})).
			Add(random.WithEvent(&domain.StockItemCreated{})).
			Add(random.WithEvent(&domain.StockItemCreated{})).
			Add(random.WithEvent(&domain.StockItemCreated{})).
			Add(random.WithEvent(&domain.StockItemCreated{})).
			Build()

		err := db.Save(context.Background(), msgs)
		require.NoError(t, err)

		output, xerr := db.db.Scan(&dynamodb.ScanInput{
			TableName: &db.eventstoreTable,
		})

		require.NoError(t, xerr)
		require.Equal(t, int64(5), *output.Count)
	})
}
