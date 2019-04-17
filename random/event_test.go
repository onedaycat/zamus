package random

import (
    "testing"
    "time"

    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestEventMsg(t *testing.T) {
    var msg *event.Msg

    msg = EventMsg().
        AggregateID("f1").
        Build()
    require.Equal(t, "f1", msg.AggID)

    msg = EventMsg().
        New().
        Build()
    require.Equal(t, int64(0), msg.Seq)

    msg = EventMsg().
        Seq(10).
        Build()
    require.Equal(t, int64(10), msg.Seq)

    now := time.Now().Unix()
    msg = EventMsg().
        Time(now).
        Build()
    require.Equal(t, now, msg.Time)

    metadata := event.Metadata{"u": "u1"}
    msg = EventMsg().
        Metadata(metadata).
        Build()
    require.Equal(t, map[string]string(metadata), msg.Metadata)

    evt := &domain.StockItemCreated{Id: "1"}
    msg = EventMsg().
        Event(evt).
        Build()
    require.NotNil(t, msg)
}
