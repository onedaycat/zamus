package mongo

// import (
// 	"context"
// 	"os"
// 	"sync"
// 	"testing"
// 	"time"

// 	"github.com/mongodb/mongo-go-driver/mongo"
// 	"github.com/onedaycat/gocqrs"
// 	"github.com/onedaycat/gocqrs/example/ecom/domain/stock"
// 	"github.com/onedaycat/gocqrs/common/clock"
// 	"github.com/stretchr/testify/require"
// )

// func newDB(t *testing.T) *MongoEventStore {
// 	client, err := mongo.NewClient(os.Getenv("MONGODB_ENDPOINT"))
// 	require.NoError(t, err)
// 	ctx := context.Background()
// 	client.Connect(ctx)

// 	db := NewMongoEventStore(client, "eventsourcing_dev")

// 	db.DropSchema()
// 	err = db.CreateSchema()
// 	require.NoError(t, err)

// 	return db
// }

// func TestGet2(t *testing.T) {
// 	db := newDB(t)
// 	es := gocqrs.NewEventStore(db, nil)

// 	wg := sync.WaitGroup{}

// 	wg.Add(2)

// 	go func() {
// 		st := stock.NewStockItem("1")
// 		st.Add(10)
// 		st.Sub(5)
// 		st.Add(2)
// 		st.Add(3)
// 		if err := es.Save(st); err != nil {
// 			panic(err)
// 		}
// 		wg.Done()
// 	}()

// 	go func() {
// 		st := stock.NewStockItem("1")
// 		st.Remove()
// 		if err := es.Save(st); err != nil {
// 			panic(err)
// 		}
// 		wg.Done()
// 	}()

// 	wg.Wait()
// }

// func TestGetSnapShot(t *testing.T) {
// 	db := newDB(t)
// 	es := gocqrs.NewEventStore(db, nil)

// 	st := stock.NewStockItem("1")
// 	st.Add(10)
// 	st.Sub(5)
// 	st.Add(2)
// 	st.Add(3)
// 	st.Remove()

// 	err := es.Save(st)
// 	require.NoError(t, err)

// 	expSt := &stock.StockItem{
// 		AggregateBase: gocqrs.InitAggregate("1"),
// 		ProductID:     "1",
// 		Qty:           10,
// 	}
// 	expSt.SetVersion(5)
// 	expSt.MarkAsRemoved()

// 	err = es.GetSnapshot("1", expSt)
// 	require.NoError(t, err)
// 	require.Equal(t, expSt, st)
// }

// func TestGet(t *testing.T) {
// 	db := newDB(t)
// 	es := gocqrs.NewEventStore(db, nil)
// 	now := time.Now()

// 	// Step 1
// 	clock.Freeze(now)
// 	st := stock.NewStockItem("1")
// 	st.Add(10)
// 	st.Sub(5)

// 	err := es.Save(st)
// 	require.NoError(t, err)

// 	// Step 2
// 	clock.Freeze(now.Add(time.Second * 5))
// 	err = es.Get("1", st)
// 	require.NoError(t, err)
// 	st.Add(2)
// 	st.Add(3)
// 	st.Add(6)
// 	st.Remove()

// 	err = es.Save(st)
// 	require.NoError(t, err)

// 	// Assert
// 	events, err := db.Get("1", now.Unix())
// 	require.NoError(t, err)
// 	require.Len(t, events, 5)
// }
