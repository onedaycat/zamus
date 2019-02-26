package random

import (
	"encoding/json"
	"time"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/gocqrs/common/eid"
	"github.com/onedaycat/zamus/eventstore"
)

type eventBuilder struct {
	msg *eventstore.EventMsg
}

func EventMsg() *eventBuilder {
	aggid := eid.GenerateID()
	seq := int64(random.Number(1, 10))
	t := time.Now().Unix()
	eventID := eid.CreateEventID(aggid, seq)

	return &eventBuilder{
		msg: &eventstore.EventMsg{
			EventID:      eventID,
			EventType:    random.SillyName(),
			EventVersion: "1",
			AggregateID:  aggid,
			Time:         t,
			Seq:          seq,
			TimeSeq:      eventstore.TimeSeq(t, seq),
		},
	}
}

func (b *eventBuilder) Build() *eventstore.EventMsg {
	return b.msg
}

func (b *eventBuilder) EventType(eventType string) *eventBuilder {
	b.msg.EventType = eventType

	return b
}

func (b *eventBuilder) AggregateID(aggid string) *eventBuilder {
	b.msg.AggregateID = aggid

	return b
}

func (b *eventBuilder) New() *eventBuilder {
	b.Seq(0)

	return b
}

func (b *eventBuilder) Seq(seq int64) *eventBuilder {
	b.msg.Seq = seq
	b.msg.TimeSeq = eventstore.TimeSeq(b.msg.Time, b.msg.Seq)
	b.msg.EventID = eid.CreateEventID(b.msg.AggregateID, b.msg.Seq)

	return b
}

func (b *eventBuilder) Time(t int64) *eventBuilder {
	b.msg.Time = t
	b.msg.TimeSeq = eventstore.TimeSeq(b.msg.Time, b.msg.Seq)

	return b
}

func (b *eventBuilder) Metadata(metadata *eventstore.Metadata) *eventBuilder {
	b.msg.Metadata, _ = metadata.Marshal()

	return b
}

func (b *eventBuilder) Versionn(version string) *eventBuilder {
	b.msg.EventVersion = version

	return b
}

func (b *eventBuilder) Event(eventType string, event interface{}) *eventBuilder {
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}

	b.msg.Event = data
	b.msg.EventType = eventType

	return b
}
