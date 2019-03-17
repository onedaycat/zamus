package random

import (
	"testing"
	"time"

	"github.com/golang/snappy"

	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/eventstore"

	"github.com/stretchr/testify/require"
)

func TestEventMsgs(t *testing.T) {
	var msgs []*eventstore.EventMsg

	event := map[string]interface{}{
		"id": "1",
	}
	eventData, _ := common.MarshalJSON(event)
	var eventDataSnap []byte
	eventDataSnap = snappy.Encode(eventDataSnap, eventData)

	metadata := &eventstore.Metadata{
		UserID: "u1",
	}
	metadataByte, _ := metadata.Marshal()

	now := time.Now().Unix()

	msgs = EventMsgs().
		Add(WithEvent("f1", event)).
		Add(WithEvent("f2", event), WithAggregateID("a1")).
		Add(WithEvent("f3", event), WithMetadata(metadata)).
		Add(WithEvent("f4", event), WithTime(now)).
		Add(WithEvent("f5", event), WithVersion("2")).
		Add(WithEvent("f6", event), WithEvent("et1")).
		Build()
	require.Len(t, msgs, 6)
	require.Equal(t, "f1", msgs[0].EventType)
	require.Equal(t, "f2", msgs[1].EventType)
	require.Equal(t, "f3", msgs[2].EventType)
	require.Equal(t, "f4", msgs[3].EventType)
	require.Equal(t, "f5", msgs[4].EventType)
	require.Equal(t, "et1", msgs[5].EventType)
	require.Equal(t, eventDataSnap, msgs[0].Event)
	require.Equal(t, eventDataSnap, msgs[1].Event)
	require.Equal(t, eventDataSnap, msgs[2].Event)
	require.Equal(t, eventDataSnap, msgs[3].Event)
	require.Equal(t, metadataByte, msgs[2].Metadata)
	require.Equal(t, now, msgs[3].Time)
	require.Equal(t, "2", msgs[4].EventVersion)

	msgsByte := EventMsgs().
		Add(WithEvent("f1", event)).
		Add(WithEvent("f2", event), WithAggregateID("a1")).
		Add(WithEvent("f3", event), WithMetadata(metadata)).
		Add(WithEvent("f4", event), WithTime(now)).
		BuildJSON()

	require.NotNil(t, msgsByte)
}
