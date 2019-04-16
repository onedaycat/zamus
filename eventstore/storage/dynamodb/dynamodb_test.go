// +build integration

package dynamodb

import (
    "context"
    "fmt"
    "testing"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/errgroup"
    "github.com/onedaycat/zamus/common/clock"
    "github.com/onedaycat/zamus/common/ptr"
    "github.com/onedaycat/zamus/common/random"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/eventstore"
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

        _db = New(sess, "eventstore", "snapshot")
        err = _db.CreateSchema(true)
        if err != nil {
            panic(err)
        }
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
        require.Equal(t, msgs[0].Event, msgsResult[0].Event)
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
        fmt.Println(msgsResult)
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

        v1 := &eventstore.Snapshot{
            AggregateID: "a1",
            Aggregate:   nil,
            EventID:     "e1",
            Time:        clock.Now().Unix(),
            Seq:         10,
            Version:     1,
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

        v1 := &eventstore.Snapshot{
            AggregateID: "a1",
            Aggregate:   nil,
            EventID:     "e1",
            Time:        clock.Now().Unix(),
            Seq:         10,
            Version:     1,
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

        v1 := &eventstore.Snapshot{
            AggregateID: "a1",
            Aggregate:   nil,
            EventID:     "e1",
            Time:        clock.Now().Unix(),
            Seq:         10,
            Version:     1,
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

        v1 := &eventstore.Snapshot{
            AggregateID: "a1",
            Aggregate:   nil,
            EventID:     "e1",
            Time:        clock.Now().Unix(),
            Seq:         10,
            Version:     1,
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

        v1 := &eventstore.Snapshot{
            AggregateID: "a1",
            Aggregate:   nil,
            EventID:     "e1",
            Time:        clock.Now().Unix(),
            Seq:         10,
            Version:     1,
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

            v1 := &eventstore.Snapshot{
                AggregateID: "a1",
                Aggregate:   nil,
                EventID:     "e1",
                Time:        clock.Now().Unix(),
                Seq:         10,
                Version:     1,
            }

            return db.Save(context.Background(), msgs, v1)
        })

        wg.Go(func() errors.Error {
            msgs := random.EventMsgs().
                Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
                Add(random.WithEvent(&domain.StockItemCreated{}), random.WithAggregateID("a1")).
                Build()

            v1 := &eventstore.Snapshot{
                AggregateID: "a1",
                Aggregate:   nil,
                EventID:     "e1",
                Time:        clock.Now().Unix(),
                Seq:         10,
                Version:     1,
            }

            return db.Save(context.Background(), msgs, v1)
        })

        err := wg.Wait()
        require.Equal(t, appErr.ErrVersionInconsistency, err)
    })
}
