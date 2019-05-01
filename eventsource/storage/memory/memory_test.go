package memory

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventsource"
	"github.com/onedaycat/zamus/internal/common/clock"
	"github.com/onedaycat/zamus/random"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *EventSourceStroage

func getDB() *EventSourceStroage {
	if _db == nil {
		_db = New()
	}

	return _db
}

func TestGetEvents(t *testing.T) {
	t.Run("From seq 0", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		msgs := random.EventMsgs().
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Build()

		err := db.saveEvents(context.Background(), msgs)
		require.NoError(t, err)
		msgsResult, err := db.GetEvents(context.Background(), "a1", 0)
		require.NoError(t, err)
		require.Len(t, msgsResult, 5)
		require.Equal(t, msgs, msgsResult)
	})

	t.Run("From seq 3", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		msgs := random.EventMsgs().
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Build()

		err := db.saveEvents(context.Background(), msgs)
		require.NoError(t, err)
		msgsResult, err := db.GetEvents(context.Background(), "a1", 3)
		require.NoError(t, err)
		require.Len(t, msgsResult, 2)
		require.Equal(t, msgs[3:], msgsResult)
	})

	t.Run("From 10", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		msgs := random.EventMsgs().
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Build()

		err := db.saveEvents(context.Background(), msgs)
		require.NoError(t, err)
		msgsResult, err := db.GetEvents(context.Background(), "a1", 10)
		require.NoError(t, err)
		require.Len(t, msgsResult, 0)
		require.Nil(t, msgsResult)
	})

	t.Run("Not Found", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		msgs := random.EventMsgs().
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Build()

		err := db.saveEvents(context.Background(), msgs)
		require.NoError(t, err)
		msgsResult, err := db.GetEvents(context.Background(), "a2", 0)
		require.NoError(t, err)
		require.Len(t, msgsResult, 0)
		require.Nil(t, msgsResult)
	})
}

func TestGetSnapshot(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		v1 := &eventsource.Snapshot{
			AggID:      "a1",
			Agg:        nil,
			EventMsgID: "e1",
			Time:       clock.Now().Unix(),
			Seq:        10,
			Version:    1,
		}

		err := db.saveSnapshot(context.Background(), v1)
		require.NoError(t, err)

		snapshot, err := db.GetSnapshot(context.Background(), "a1", 1)
		require.NoError(t, err)
		require.Equal(t, v1, snapshot)
	})

	t.Run("Aggregate ID Not Found", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		v1 := &eventsource.Snapshot{
			AggID:      "a1",
			Agg:        nil,
			EventMsgID: "e1",
			Time:       clock.Now().Unix(),
			Seq:        10,
			Version:    1,
		}

		err := db.saveSnapshot(context.Background(), v1)
		require.NoError(t, err)

		snapshot, err := db.GetSnapshot(context.Background(), "a2", 1)
		require.NoError(t, err)
		require.Nil(t, snapshot)
	})

	t.Run("Version Not Found", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		v1 := &eventsource.Snapshot{
			AggID:      "a1",
			Agg:        nil,
			EventMsgID: "e1",
			Time:       clock.Now().Unix(),
			Seq:        10,
			Version:    1,
		}

		err := db.saveSnapshot(context.Background(), v1)
		require.NoError(t, err)

		snapshot, err := db.GetSnapshot(context.Background(), "a1", 2)
		require.NoError(t, err)
		require.Nil(t, snapshot)
	})

	t.Run("Version 0", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		v1 := &eventsource.Snapshot{
			AggID:      "a1",
			Agg:        nil,
			EventMsgID: "e1",
			Time:       clock.Now().Unix(),
			Seq:        10,
			Version:    1,
		}

		err := db.saveSnapshot(context.Background(), v1)
		require.NoError(t, err)

		snapshot, err := db.GetSnapshot(context.Background(), "a1", 2)
		require.NoError(t, err)
		require.Nil(t, snapshot)
	})

	t.Run("Nil Snapshot", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		err := db.saveSnapshot(context.Background(), nil)
		require.NoError(t, err)

		snapshot, err := db.GetSnapshot(context.Background(), "a1", 2)
		require.NoError(t, err)
		require.Nil(t, snapshot)
	})
}

func TestSave(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db := getDB()
		db.Truncate()

		msgs := random.EventMsgs().
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
			Build()

		v1 := &eventsource.Snapshot{
			AggID:      "a1",
			Agg:        nil,
			EventMsgID: "e1",
			Time:       clock.Now().Unix(),
			Seq:        10,
			Version:    1,
		}

		err := db.Save(context.Background(), msgs, v1)
		require.NoError(t, err)

		msgsResult, err := db.GetEvents(context.Background(), "a1", 0)
		require.NoError(t, err)
		require.Equal(t, msgs, msgsResult)

		snapshot, err := db.GetSnapshot(context.Background(), "a1", 1)
		require.NoError(t, err)
		require.Equal(t, v1, snapshot)
	})

	t.Run("Concurency", func(t *testing.T) {
		db := getDB()
		db.Truncate()
		wg := errgroup.Group{}

		wg.Go(func() errors.Error {
			msgs := random.EventMsgs().
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Build()

			v1 := &eventsource.Snapshot{
				AggID:      "a1",
				Agg:        nil,
				EventMsgID: "e1",
				Time:       clock.Now().Unix(),
				Seq:        10,
				Version:    1,
			}

			return db.Save(context.Background(), msgs, v1)
		})

		wg.Go(func() errors.Error {
			msgs := random.EventMsgs().
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
				Build()

			v1 := &eventsource.Snapshot{
				AggID:      "a1",
				Agg:        nil,
				EventMsgID: "e1",
				Time:       clock.Now().Unix(),
				Seq:        10,
				Version:    1,
			}

			return db.Save(context.Background(), msgs, v1)
		})

		err := wg.Wait()
		require.Equal(t, appErr.ErrVersionInconsistency, err)
	})
}
