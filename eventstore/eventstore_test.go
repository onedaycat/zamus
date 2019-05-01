package eventstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/eventstore/mocks"
	"github.com/onedaycat/zamus/eventstore/storage/memory"
	"github.com/onedaycat/zamus/internal/common/clock"
	"github.com/onedaycat/zamus/internal/common/eid"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

var _db *memory.EventStoreStroage

func getDB() *memory.EventStoreStroage {
	if _db == nil {
		_db = memory.New()
	}

	return _db
}

type EventStoreSuite struct {
	mockStore *mocks.Storage
	es        eventstore.EventStore
	ctx       context.Context
	db        eventstore.Storage
}

func setupEventStoreSuite() *EventStoreSuite {
	s := &EventStoreSuite{}
	db := getDB()
	db.Truncate()
	s.ctx = context.Background()
	s.es = eventstore.New(db)

	return s
}

func (s *EventStoreSuite) WithMockStorage() *EventStoreSuite {
	s.mockStore = &mocks.Storage{}
	s.es = eventstore.New(s.mockStore)

	return s
}

func (s *EventStoreSuite) WithMockStorageAndSnapshot() *EventStoreSuite {
	s.mockStore = &mocks.Storage{}
	s.es = eventstore.New(s.mockStore)

	return s
}

func TestWorkFlow(t *testing.T) {
	t.Run("New Aggregate", func(t *testing.T) {
		s := setupEventStoreSuite()

		st := domain.NewStockItem()

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
		err := s.es.Save(s.ctx, st)
		require.NoError(t, err)
		require.Len(t, st.GetEvents(), 0)
	})
}

func TestSaveAndGet(t *testing.T) {
	db := getDB()
	db.Truncate()
	ctx := context.Background()
	es := eventstore.New(db)

	now1 := time.Now().UTC().Add(time.Second * -10)

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
}

func TestPubishEvents(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db := getDB()
		ctx := context.Background()

		db.Truncate()
		es := eventstore.New(db)

		eid.FreezeID("id1")

		events := []event.Event{
			&domain.StockItemCreated{
				Id:        "id1",
				ProductID: "p1",
				Qty:       1,
			},
			&domain.StockItemRemoved{
				ProductID: "p1",
				RemovedAt: 1,
			},
		}

		err := es.PublishEvents(ctx, events...)
		require.NoError(t, err)
	})
}
