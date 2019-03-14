package random

import (
	"testing"
	"time"

	"github.com/golang/snappy"
	jsoniter "github.com/json-iterator/go"

	"github.com/onedaycat/zamus/eventstore"

	"github.com/stretchr/testify/require"
)

func TestEventMsgs(t *testing.T) {
	var msgs []*eventstore.EventMsg

	event := map[string]interface{}{
		"id": "1",
	}
	eventData, _ := jsoniter.ConfigFastest.Marshal(event)
	var eventDataSnap []byte
	eventDataSnap = snappy.Encode(eventDataSnap, eventData)

	metadata := &eventstore.Metadata{
		UserID: "u1",
	}
	metadataByte, _ := metadata.Marshal()

	now := time.Now().Unix()

	msgs = EventMsgs().
		Add("f1", event).
		Add("f2", event, WithAggregateID("a1")).
		Add("f3", event, WithMetadata(metadata)).
		Add("f4", event, WithTime(now)).
		Build()
	require.Len(t, msgs, 4)
	require.Equal(t, "f1", msgs[0].EventType)
	require.Equal(t, "f2", msgs[1].EventType)
	require.Equal(t, "f3", msgs[2].EventType)
	require.Equal(t, "f4", msgs[3].EventType)
	require.Equal(t, eventDataSnap, msgs[0].Event)
	require.Equal(t, eventDataSnap, msgs[1].Event)
	require.Equal(t, eventDataSnap, msgs[2].Event)
	require.Equal(t, eventDataSnap, msgs[3].Event)
	require.Equal(t, metadataByte, msgs[2].Metadata)
	require.Equal(t, now, msgs[3].Time)

	msgsByte := EventMsgs().
		Add("f1", event).
		Add("f2", event, WithAggregateID("a1")).
		Add("f3", event, WithMetadata(metadata)).
		Add("f4", event, WithTime(now)).
		BuildJSON()

	require.NotNil(t, msgsByte)
}
