package eventsource_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/eventsource"
	"github.com/onedaycat/zamus/eventsource/mocks"
	"github.com/onedaycat/zamus/eventsource/storage/memory"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/internal/common/clock"
	"github.com/onedaycat/zamus/internal/common/eid"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *memory.EventSourceStroage

func getDB() *memory.EventSourceStroage {
	if _db == nil {
		_db = memory.New()
	}

	return _db
}

type EventSourceSuite struct {
	mockStore *mocks.Storage
	es        eventsource.EventSource
	ctx       context.Context
	db        eventsource.Storage
}

func setupEventSourceSuite() *EventSourceSuite {
	s := &EventSourceSuite{}
	db := getDB()
	db.Truncate()
	s.ctx = context.Background()
	s.es = eventsource.New(db)

	return s
}

func (s *EventSourceSuite) WithSnapshot() *EventSourceSuite {
	s.es = eventsource.New(getDB(), eventsource.WithSnapshot(1))

	return s
}

func (s *EventSourceSuite) WithMockStorage() *EventSourceSuite {
	s.mockStore = &mocks.Storage{}
	s.es = eventsource.New(s.mockStore)

	return s
}

func (s *EventSourceSuite) WithMockStorageAndSnapshot() *EventSourceSuite {
	s.mockStore = &mocks.Storage{}
	s.es = eventsource.New(s.mockStore, eventsource.WithSnapshot(1))

	return s
}

func TestWorkFlow(t *testing.T) {
	t.Run("New Aggregate", func(t *testing.T) {
		s := setupEventSourceSuite()

		st := domain.NewStockItem()
		err := s.es.GetAggregate(s.ctx, "a1", st)
		require.NoError(t, err)
		require.True(t, st.IsNew())

		st.Create("a1", "p1", 1)
		require.Equal(t, 1, len(st.GetEvents()))
		require.Equal(t, &domain.StockItemCreated{
			Id:        "a1",
			ProductID: "p1",
			Qty:       1,
		}, st.GetEvents()[0])

		eid.FreezeID("xxx")
		now := time.Now()
		clock.Freeze(now)
		err = s.es.Save(s.ctx, st)
		require.NoError(t, err)
		require.Equal(t, int64(1), st.GetSequence())
		require.Len(t, st.GetEvents(), 0)

		st2 := domain.NewStockItem()
		err = s.es.GetAggregate(s.ctx, "a1", st2)
		require.NoError(t, err)
		require.Equal(t, st2, st)
	})

	t.Run("Exist Aggregate", func(t *testing.T) {
		s := setupEventSourceSuite()

		st := domain.NewStockItem()
		err := s.es.GetAggregate(s.ctx, "a1", st)
		require.NoError(t, err)
		require.True(t, st.IsNew())

		st.Create("a1", "p1", 1)
		require.Equal(t, 1, len(st.GetEvents()))
		require.Equal(t, &domain.StockItemCreated{
			Id:        "a1",
			ProductID: "p1",
			Qty:       1,
		}, st.GetEvents()[0])

		eid.FreezeID("xxx")

		now := time.Now()
		clock.Freeze(now)
		err = s.es.Save(s.ctx, st)

		st2 := domain.NewStockItem()
		err = s.es.GetAggregate(s.ctx, "a1", st2)
		require.NoError(t, err)
		require.Equal(t, st2, st)

		st2.Add(3)
		st2.Add(5)
		err = s.es.Save(s.ctx, st2)
		require.NoError(t, err)
		require.Equal(t, int64(3), st2.GetSequence())
		require.Len(t, st.GetEvents(), 0)

		st3 := domain.NewStockItem()
		err = s.es.GetAggregate(s.ctx, "a1", st3)
		require.NoError(t, err)
		require.Equal(t, st3, st2)
	})
}

func TestGetAggregate(t *testing.T) {
	t.Run("No Snapshot No Event", func(t *testing.T) {
		s := setupEventSourceSuite().
			WithMockStorage()

		st := domain.NewStockItem()

		s.mockStore.On("GetSnapshot", s.ctx, "a1", 1).Return(nil, nil)
		s.mockStore.On("GetEvents", s.ctx, "a1", int64(0)).Return(nil, nil)

		err := s.es.GetAggregate(s.ctx, "a1", st)
		require.NoError(t, err)
		require.True(t, st.IsNew())
	})

	t.Run("Has Snapshot No Event", func(t *testing.T) {
		s := setupEventSourceSuite().
			WithMockStorageAndSnapshot()

		st := domain.NewStockItem()
		stByte, err := common.MarshalJSON(st)
		require.NoError(t, err)

		var dst []byte
		dst = snappy.Encode(dst, stByte)

		now := time.Now()
		clock.Freeze(now)

		snapshot := &eventsource.Snapshot{
			AggID:      "a1",
			Agg:        dst,
			EventMsgID: "a1:1",
			Time:       now.Unix(),
			Seq:        1,
			Version:    1,
		}

		s.mockStore.On("GetSnapshot", s.ctx, "a1", 1).Return(snapshot, nil)
		s.mockStore.On("GetEvents", s.ctx, "a1", int64(1)).Return(nil, nil)

		err = s.es.GetAggregate(s.ctx, "a1", st)
		require.NoError(t, err)
		require.False(t, st.IsNew())
	})
}

func TestSaveAndGet(t *testing.T) {
	db := getDB()
	db.Truncate()
	ctx := context.Background()
	es := eventsource.New(db)

	now1 := time.Now().UTC().Add(time.Second * -10)
	now2 := time.Now().UTC().Add(time.Second * -5)

	id := eid.GenerateID()
	st := domain.NewStockItem()
	st.Create(id, "1", 0)
	st.Add(10)
	_ = st.Sub(5)
	st.Add(2)
	st.Add(3)

	clock.Freeze(now1)
	err := es.Save(ctx, st)
	require.NoError(t, err)
	lastSeq := st.GetSequence()

	// GetAggregate
	st2 := domain.NewStockItem()
	err = es.GetAggregate(ctx, st.GetAggregateID(), st2)
	require.NoError(t, err)
	require.Equal(t, st, st2)

	// GetGetEvents
	st.Add(2)
	_ = st.Remove()

	require.True(t, st.IsRemoved())

	clock.Freeze(now2)

	meta := event.Metadata{"u": "u1"}
	ctx = event.NewMetadataContext(ctx, meta)

	err = es.Save(ctx, st)
	require.NoError(t, err)

	events, err := es.GetEvents(ctx, st.GetAggregateID(), lastSeq)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "testdata.stock.v1.StockItemUpdated", events[0].EventType)
	require.Equal(t, int64(6), events[0].Seq)
	require.Equal(t, "testdata.stock.v1.StockItemRemoved", events[1].EventType)
	require.Equal(t, int64(7), events[1].Seq)
	require.Equal(t, meta, events[0].Metadata)

	// GetAggregateByTimeSeq
	st4 := domain.NewStockItem()
	err = es.GetAggregateBySeq(ctx, st.GetAggregateID(), st4, 0)
	require.NoError(t, err)
	require.Equal(t, st4, st)
}

func TestNotFound(t *testing.T) {
	db := getDB()
	ctx := context.Background()

	es := eventsource.New(db)

	// GetAggregate
	st := domain.NewStockItem()
	st.SetAggregateID("1x")
	err := es.GetAggregate(ctx, st.GetAggregateID(), st)
	require.NoError(t, err)
	require.True(t, st.IsNew())

	// GetEvents
	msgs, err := es.GetEvents(ctx, st.GetAggregateID(), 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, msgs)

	st4 := domain.NewStockItem()
	err = es.GetAggregateBySeq(ctx, st.GetAggregateID(), st4, 1)
	require.NoError(t, err)
	require.True(t, st4.IsNew())
}

func TestConcurency(t *testing.T) {
	db := getDB()
	ctx := context.Background()

	db.Truncate()
	es := eventsource.New(db)

	wg := sync.WaitGroup{}
	wg.Add(2)

	var err1 error
	var err2 error
	go func() {
		st := domain.NewStockItem()
		st.Create("a1", "1", 0)
		st.Add(10)
		_ = st.Sub(5)
		st.Add(2)
		st.Add(3)

		err1 = es.Save(ctx, st)

		wg.Done()
	}()

	go func() {
		st := domain.NewStockItem()
		st.Create("a1", "1", 0)
		st.Add(1)
		_ = st.Remove()

		err2 = es.Save(ctx, st)

		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		require.Equal(t, errors.ErrVersionInconsistency, err1)
		require.Nil(t, err2)
	} else {
		require.Equal(t, errors.ErrVersionInconsistency, err2)
		require.Nil(t, err1)
	}

}

func TestPubishEvents(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db := getDB()
		ctx := context.Background()

		db.Truncate()
		es := eventsource.New(db)

		eid.FreezeID("id1")

		events := []event.Event{
			&domain.StockItemCreated{
				Id:        "id1",
				ProductID: "p1",
				Qty:       1,
			},
			&domain.StockItemCreated{
				ProductID: "p1",
			},
		}

		err := es.PublishEvents(ctx, events...)
		require.NoError(t, err)

		getEvents, err := es.GetEvents(ctx, "testdata.stock.v1.StockItemCreated", 0)
		require.NoError(t, err)
		require.Len(t, getEvents, 2)
		require.Equal(t, "testdata.stock.v1.StockItemCreated", getEvents[0].EventType)
		require.Equal(t, "testdata.stock.v1.StockItemCreated", getEvents[1].EventType)
	})
}
