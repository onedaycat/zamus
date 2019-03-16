package random

import (
	"testing"
	"time"

	"github.com/golang/snappy"

	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/stretchr/testify/require"
)

func TestEventMsg(t *testing.T) {
	var msg *eventstore.EventMsg

	msg = EventMsg().
		EventType("f1").
		Build()
	require.Equal(t, "f1", msg.EventType)

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

	metadata := &eventstore.Metadata{
		UserID: "u1",
	}
	metadataByte, _ := metadata.Marshal()
	msg = EventMsg().
		Metadata(metadata).
		Build()
	require.Equal(t, metadataByte, msg.Metadata)

	event := map[string]interface{}{
		"id": "1",
	}

	eventByte, _ := common.MarshalJSON(event)
	var eventDataSnap []byte
	eventDataSnap = snappy.Encode(eventDataSnap, eventByte)
	msg = EventMsg().
		Event("e1", event).
		Build()
	require.Equal(t, eventDataSnap, msg.Event)
	require.Equal(t, "e1", msg.EventType)

	msg = EventMsg().
		Versionn("2").
		Build()
	require.Equal(t, "2", msg.EventVersion)

	msgByte := EventMsg().
		Versionn("2").
		BuildJSON()
	require.NotNil(t, msgByte)
}
