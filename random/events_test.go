package random

import (
    "testing"
    "time"

    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestEventMsgs(t *testing.T) {
    var msgs event.Msgs

    evt := &domain.StockItemCreated{Id: "1"}
    eventAny, _ := event.MarshalEvent(evt)

    metadata := event.Metadata{"u": "u1"}

    now := time.Now().Unix()

    msgs = EventMsgs().
        Add(WithEvent(evt)).
        Add(WithEvent(evt), WithAggregateID("a1")).
        Add(WithEvent(evt), WithMetadata(metadata)).
        Add(WithEvent(evt), WithTime(now)).
        Build()
    require.Len(t, msgs, 4)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[0].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[1].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[2].EventType)
    require.Equal(t, "testdata.stock.v1.StockItemCreated", msgs[3].EventType)
    require.Equal(t, eventAny, msgs[0].Event)
    require.Equal(t, eventAny, msgs[1].Event)
    require.Equal(t, eventAny, msgs[2].Event)
    require.Equal(t, map[string]string(metadata), msgs[2].Metadata)
    require.Equal(t, now, msgs[3].Time)

    msgsByte := EventMsgs().
        Add(WithEvent(evt)).
        Add(WithEvent(evt), WithAggregateID("a1")).
        Add(WithEvent(evt), WithMetadata(metadata)).
        Add(WithEvent(evt), WithTime(now)).
        BuildJSON()

    require.NotNil(t, msgsByte)
}
