package random

import (
    "time"

    random "github.com/Pallinder/go-randomdata"
    "github.com/gogo/protobuf/proto"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/internal/common/eid"
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

func (b *eventBuilder) Metadata(metadata eventstore.Metadata) *eventBuilder {
    b.msg.Metadata = metadata

    return b
}

func (b *eventBuilder) Versionn(version string) *eventBuilder {
    b.msg.EventVersion = version

    return b
}

func (b *eventBuilder) Event(evt proto.Message) *eventBuilder {
    data, err := common.MarshalEvent(evt)
    if err != nil {
        panic(err)
    }

    b.msg.Event = data
    b.msg.EventType = proto.MessageName(evt)

    return b
}

func (b *eventBuilder) BuildProto() []byte {
    data, err := common.MarshalEventMsg(b.msg)
    if err != nil {
        panic(err)
    }

    return data
}

func (b *eventBuilder) BuildJSON() []byte {
    data, err := common.MarshalJSON(b.msg)
    if err != nil {
        panic(err)
    }

    return data
}
