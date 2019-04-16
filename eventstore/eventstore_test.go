package eventstore_test

import (
    "context"
    "sync"
    "testing"
    "time"

    "github.com/golang/snappy"
    "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/eventstore/mocks"
    "github.com/onedaycat/zamus/eventstore/storage/memory"
    "github.com/onedaycat/zamus/internal/common"
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
}

func setupEventStoreSuite() *EventStoreSuite {
    s := &EventStoreSuite{}
    db := getDB()
    db.Truncate()
    s.ctx = context.Background()
    s.es = eventstore.NewEventStore(db)

    return s
}

func (s *EventStoreSuite) WithMockStorage() *EventStoreSuite {
    s.mockStore = &mocks.Storage{}
    s.es = eventstore.NewEventStore(s.mockStore)

    return s
}

func TestWorkFlow(t *testing.T) {
    t.Run("New Aggregate", func(t *testing.T) {
        s := setupEventStoreSuite()

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

        now := time.Now()
        clock.Freeze(now)
        err = s.es.Save(s.ctx, st)
        require.NoError(t, err)
        require.Equal(t, int64(1), st.GetSequence())
        require.Len(t, st.GetEvents(), 0)
        require.Equal(t, now.Unix(), st.GetLastEventTime())
        require.Equal(t, "a1:1", st.GetLastEventID())

        st2 := domain.NewStockItem()
        err = s.es.GetAggregate(s.ctx, "a1", st2)
        require.NoError(t, err)
        require.Equal(t, st2, st)
    })

    t.Run("Exist Aggregate", func(t *testing.T) {
        s := setupEventStoreSuite()

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
        require.Equal(t, now.Unix(), st2.GetLastEventTime())
        require.Equal(t, "a1:3", st2.GetLastEventID())

        st3 := domain.NewStockItem()
        err = s.es.GetAggregate(s.ctx, "a1", st3)
        require.NoError(t, err)
        require.Equal(t, st3, st2)
    })
}

func TestGetAggregate(t *testing.T) {
    t.Run("No Snapshot No Event", func(t *testing.T) {
        s := setupEventStoreSuite().
            WithMockStorage()

        st := domain.NewStockItem()

        s.mockStore.On("GetSnapshot", s.ctx, "a1", 1).Return(nil, nil)
        s.mockStore.On("GetEvents", s.ctx, "a1", int64(0)).Return(nil, nil)

        err := s.es.GetAggregate(s.ctx, "a1", st)
        require.NoError(t, err)
        require.True(t, st.IsNew())
    })

    t.Run("Has Snapshot No Event", func(t *testing.T) {
        s := setupEventStoreSuite().
            WithMockStorage()

        st := domain.NewStockItem()
        stByte, err := common.MarshalJSON(st)
        require.NoError(t, err)

        var dst []byte
        dst = snappy.Encode(dst, stByte)

        now := time.Now()
        clock.Freeze(now)

        snapshot := &eventstore.Snapshot{
            AggregateID: "a1",
            Aggregate:   dst,
            EventID:     "a1:1",
            Time:        now.Unix(),
            Seq:         1,
            Version:     1,
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
    es := eventstore.NewEventStore(db)

    now1 := time.Now().UTC().Add(time.Second * -10)
    now2 := time.Now().UTC().Add(time.Second * -5)

    metadata := eventstore.NewMetadata().SetUserID("u1")

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
    err = es.SaveWithMetadata(ctx, st, metadata)
    require.NoError(t, err)

    events, err := es.GetEvents(ctx, st.GetAggregateID(), lastSeq)
    require.NoError(t, err)
    require.Len(t, events, 2)
    require.Equal(t, "testdata.stock.v1.StockItemUpdated", events[0].EventType)
    require.Equal(t, int64(6), events[0].Seq)
    require.Equal(t, "testdata.stock.v1.StockItemRemoved", events[1].EventType)
    require.Equal(t, int64(7), events[1].Seq)
    require.Equal(t, map[string]string(metadata), events[0].Metadata)

    // GetAggregateByTimeSeq
    st4 := domain.NewStockItem()
    err = es.GetAggregateBySeq(ctx, st.GetAggregateID(), st4, 0)
    require.NoError(t, err)
    require.Equal(t, st4, st)
}

func TestNotFound(t *testing.T) {
    db := getDB()
    ctx := context.Background()

    es := eventstore.NewEventStore(db)

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
    es := eventstore.NewEventStore(db)

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
        es := eventstore.NewEventStore(db)

        eid.FreezeID("id1")

        st := &domain.StockItemCreated{
            Id:        "id1",
            ProductID: "p1",
            Qty:       1,
        }

        event := &eventstore.EventPublish{
            Event: st,
        }

        err := es.PublishEvents(ctx, event)
        require.NoError(t, err)

        getEvents, err := es.GetEvents(ctx, "id1", 0)
        require.NoError(t, err)
        require.Len(t, getEvents, 1)
    })

    t.Run("SuccessWithAggAndSeq", func(t *testing.T) {
        db := getDB()
        ctx := context.Background()

        db.Truncate()
        es := eventstore.NewEventStore(db)

        eid.UnFreezeID()
        now := time.Now().UTC()

        events := []*eventstore.EventPublish{
            {
                AggregateID: "id1",
                Seq:         now.Add(time.Second * 1).Unix(),
                Event: &domain.StockItemCreated{
                    Id:        "id1",
                    ProductID: "p1",
                    Qty:       1,
                },
            },
            {
                AggregateID: "id1",
                Seq:         now.Add(time.Second * 2).Unix(),
                Event: &domain.StockItemRemoved{
                    ProductID: "p1",
                    RemovedAt: 1,
                },
            },
        }

        err := es.PublishEvents(ctx, events...)
        require.NoError(t, err)

        getEvents, err := es.GetEvents(ctx, "id1", 0)
        require.NoError(t, err)
        require.Len(t, getEvents, 2)
        require.Equal(t, "testdata.stock.v1.StockItemCreated", getEvents[0].EventType)
        require.Equal(t, "testdata.stock.v1.StockItemRemoved", getEvents[1].EventType)
    })
}
