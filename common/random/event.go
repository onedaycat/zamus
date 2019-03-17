package random

import (
	"time"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/eid"
	"github.com/onedaycat/zamus/eventstore"
)

type eventBuilder struct {
	msg *eventstore.EventMsg
}

func EventMsg() *eventBuilder {
	aggid := eid.GenerateID()
	t := time.Now().Unix()
	eventID := eid.CreateEventID(aggid, 1)

	return &eventBuilder{
		msg: &eventstore.EventMsg{
			EventID:      eventID,
			EventType:    random.SillyName(),
			EventVersion: "1",
			AggregateID:  aggid,
			Time:         t,
			Seq:          1,
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
	b.msg.EventID = eid.CreateEventID(b.msg.AggregateID, b.msg.Seq)

	return b
}

func (b *eventBuilder) Time(t int64) *eventBuilder {
	b.msg.Time = t

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
	data, err := common.MarshalJSONSnappy(event)
	if err != nil {
		panic(err)
	}

	b.msg.Event = data
	b.msg.EventType = eventType

	return b
}

func (b *eventBuilder) BuildJSON() []byte {
	data, err := common.MarshalJSON(b.msg)
	if err != nil {
		panic(err)
	}

	return data
}
