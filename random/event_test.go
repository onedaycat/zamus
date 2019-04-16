package random

import (
    "testing"
    "time"

    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestEventMsg(t *testing.T) {
    var msg *eventstore.EventMsg

    msg = EventMsg().
        AggregateID("f1").
        Build()
    require.Equal(t, "f1", msg.AggregateID)

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

    metadata := eventstore.NewMetadata().SetUserID("u1")
    msg = EventMsg().
        Metadata(metadata).
        Build()
    require.Equal(t, map[string]string(metadata), msg.Metadata)

    event := &domain.StockItemCreated{Id: "1"}
    msg = EventMsg().
        Event(event).
        Build()
    require.NotNil(t, msg)

    msg = EventMsg().
        Versionn("2").
        Build()
    require.Equal(t, "2", msg.EventVersion)

    msgByte := EventMsg().
        Versionn("2").
        BuildJSON()
    require.NotNil(t, msgByte)
}
