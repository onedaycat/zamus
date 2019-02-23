package memory

import (
	"sync"
	"testing"
	"time"

	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *MemoryEventStore

func getDB() *MemoryEventStore {
	if _db == nil {
		_db = New()
	}

	return _db
}

func TestSaveAndGet(t *testing.T) {
	db := getDB()
	db.Truncate()

	es := eventstore.NewEventStore(db)

	now1 := time.Now().UTC().Add(time.Second * -10)
	now2 := time.Now().UTC().Add(time.Second * -5)

	id := eid.GenerateID()
	st := domain.NewStockItem()
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

	// GetGetEvents
	st.Add(2)
	st.Remove()
	require.True(t, st.IsRemoved())

	clock.Freeze(now2)
	err = es.Save(st)
	require.NoError(t, err)

	events, err := es.GetEvents(st.GetAggregateID(), eventstore.TimeSeq(now2.Unix(), 0))
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, domain.StockItemUpdatedEvent, events[0].EventType)
	require.Equal(t, int64(6), events[0].Seq)
	require.Equal(t, domain.StockItemRemovedEvent, events[1].EventType)
	require.Equal(t, int64(7), events[1].Seq)

	// GetAggregateByTimeSeq
	st4 := domain.NewStockItem()
	err = es.GetAggregateByTimeSeq(st.GetAggregateID(), st4, eventstore.TimeSeq(now2.Unix(), 0))
	require.NoError(t, err)
	require.Equal(t, st4, st)
}

func TestNotFound(t *testing.T) {
	db := getDB()

	es := eventstore.NewEventStore(db)

	// GetAggregate
	st := domain.NewStockItem()
	st.SetAggregateID("1x")
	err := es.GetAggregate(st.GetAggregateID(), st)
	require.Equal(t, eventstore.ErrNotFound, err)
	require.True(t, st.IsNew())

	// GetEvents
	msgs, err := es.GetEvents(st.GetAggregateID(), 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, msgs)

	st4 := domain.NewStockItem()
	err = es.GetAggregateByTimeSeq(st.GetAggregateID(), st4, eventstore.TimeSeq(clock.Now().Unix(), 1))
	require.Equal(t, eventstore.ErrNotFound, err)
	require.True(t, st4.IsNew())
}

func TestConcurency(t *testing.T) {
	db := getDB()

	db.Truncate()
	es := eventstore.NewEventStore(db)

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
		st.Add(1)
		st.Remove()

		err2 = es.Save(st)

		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, eventstore.ErrVersionInconsistency, err1)
	require.Nil(t, err2)
}