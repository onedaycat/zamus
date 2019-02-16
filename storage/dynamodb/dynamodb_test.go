package dynamodb

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/gocqrs"
	"github.com/onedaycat/gocqrs/common/clock"
	"github.com/onedaycat/gocqrs/common/eid"
	"github.com/onedaycat/gocqrs/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *DynamoDBEventStore

func getDB() *DynamoDBEventStore {
	if _db == nil {
		sess, err := session.NewSession(&aws.Config{
			Credentials: credentials.NewEnvCredentials(),
			Region:      aws.String("ap-southeast-1"),
		})
		if err != nil {
			panic(err)
		}

		_db = New(sess, "test-eventstore", "test-eventsnapshot")
		err = _db.CreateSchema(true)
		if err != nil {
			panic(err)
		}
	}

	return _db
}

func TestSaveAndGet(t *testing.T) {
	db := getDB()
	db.TruncateTables()

	es := gocqrs.NewEventStore(db)

	now1 := time.Now().UTC().Add(time.Second * -10)
	now2 := time.Now().UTC().Add(time.Second * -5)

	st := domain.NewStockItem()
	id := eid.GenerateID()
	st.Create(id, "1", 0)
	st.Add(10)
	st.Sub(5)
	st.Add(2)
	st.Add(3)

	clock.Freeze(now1)
	err := es.Save(st)
	require.NoError(t, err)

	// GetAggregate
	st2 := domain.NewStockItem()
	err = es.GetAggregate(st.GetAggregateID(), st2)
	require.NoError(t, err)
	require.Equal(t, st, st2)

	// Get
	st.Add(2)
	st.Remove()

	clock.Freeze(now2)
	err = es.Save(st)
	require.NoError(t, err)

	st3 := domain.NewStockItem()
	events, err := es.GetEvents(st.GetAggregateID(), gocqrs.NewSeq(now2.Unix(), 0), st3)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.True(t, st.IsRemoved())
	require.Equal(t, domain.StockItemUpdatedEvent, events[0].EventType)
	require.Equal(t, int64(6), events[0].Seq)
	require.Equal(t, domain.StockItemRemovedEvent, events[1].EventType)
	require.Equal(t, int64(7), events[1].Seq)

	// GetSnapshot
	st4 := domain.NewStockItem()
	err = es.GetSnapshot(st.GetAggregateID(), st4)
	require.NoError(t, err)
	require.Equal(t, st4, st)

	// GetByEventType
	events, err = es.GetEventsByEventType(domain.StockItemUpdatedEvent, gocqrs.NewSeq(now2.Unix(), 0))
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, domain.StockItemUpdatedEvent, events[0].EventType)
	require.Equal(t, int64(6), events[0].Seq)

	// GetByAggregateType
	events, err = es.GetEventsByAggregateType(st.GetAggregateType(), gocqrs.NewSeq(now2.Unix(), 0))
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.True(t, st.IsRemoved())
	require.Equal(t, domain.StockItemUpdatedEvent, events[0].EventType)
	require.Equal(t, int64(6), events[0].Seq)
	require.Equal(t, domain.StockItemRemovedEvent, events[1].EventType)
	require.Equal(t, int64(7), events[1].Seq)
}

func TestNotFound(t *testing.T) {
	db := getDB()

	es := gocqrs.NewEventStore(db)

	// GetAggregate
	st := domain.NewStockItem()
	st.SetAggregateID("1x")
	err := es.GetAggregate(st.GetAggregateID(), st)
	require.Equal(t, gocqrs.ErrNotFound, err)

	// Get
	st3 := domain.NewStockItem()
	events, err := es.GetEvents(st.GetAggregateID(), 0, st3)
	require.Nil(t, err)
	require.Nil(t, events)

	// GetSnapshot
	st4 := domain.NewStockItem()
	err = es.GetSnapshot(st.GetAggregateID(), st4)
	require.Equal(t, gocqrs.ErrNotFound, err)
	require.Nil(t, events)

	// GetByEventType
	events, err = es.GetEventsByEventType("xxxx", 0)
	require.Nil(t, err)
	require.Nil(t, events)

	// GetByAggregateType
	events, err = es.GetEventsByAggregateType("xxxx", 0)
	require.Nil(t, events)
	require.Nil(t, err)
}

func TestConcurency(t *testing.T) {
	db := getDB()

	db.TruncateTables()
	es := gocqrs.NewEventStore(db)

	wg := sync.WaitGroup{}
	wg.Add(2)

	var err1 error
	var err2 error
	go func() {
		st := domain.NewStockItem()
		st.Create("a1", "1", 0)
		st.Add(10)
		st.Sub(5)
		st.Add(2)
		st.Add(3)

		err1 = es.Save(st)

		wg.Done()
	}()

	go func() {
		st := domain.NewStockItem()
		st.Create("a1", "1", 0)
		st.Remove()

		err2 = es.Save(st)

		wg.Done()
	}()

	wg.Wait()
	require.Equal(t, gocqrs.ErrVersionInconsistency, err1)
	require.Nil(t, err2)
}
