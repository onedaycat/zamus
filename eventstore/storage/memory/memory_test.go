package memory

import (
	"context"
	"testing"

	"github.com/onedaycat/zamus/random"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *EventStoreStroage

func getDB() *EventStoreStroage {
	if _db == nil {
		_db = New()
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

		require.Equal(t, 5, len(db.eventstore))
	})
}
