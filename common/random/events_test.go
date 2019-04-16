package random

import (
    "testing"
    "time"

    "github.com/onedaycat/zamus/common"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestEventMsgs(t *testing.T) {
    var msgs []*eventstore.EventMsg

    event := &domain.StockItemCreated{Id: "1"}
    eventAny, _ := common.MarshalEvent(event)

    metadata := eventstore.NewMetadata().SetUserID("u1")

    now := time.Now().Unix()

    msgs = EventMsgs().
        Add(WithEvent(event)).
        Add(WithEvent(event), WithAggregateID("a1")).
        Add(WithEvent(event), WithMetadata(metadata)).
        Add(WithEvent(event), WithTime(now)).
        Add(WithEvent(event), WithVersion("2")).
        Build()
    require.Len(t, msgs, 5)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[0].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[1].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[2].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[3].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[4].EventType)
    require.Equal(t, eventAny, msgs[0].Event)
    require.Equal(t, eventAny, msgs[1].Event)
    require.Equal(t, eventAny, msgs[2].Event)
    require.Equal(t, eventAny, msgs[3].Event)
    require.Equal(t, map[string]string(metadata), msgs[2].Metadata)
    require.Equal(t, now, msgs[3].Time)
    require.Equal(t, "2", msgs[4].EventVersion)

    msgsByte := EventMsgs().
        Add(WithEvent(event)).
        Add(WithEvent(event), WithAggregateID("a1")).
        Add(WithEvent(event), WithMetadata(metadata)).
        Add(WithEvent(event), WithTime(now)).
        BuildJSON()

    require.NotNil(t, msgsByte)
}
