package random

import (
    "time"

    random "github.com/Pallinder/go-randomdata"
    "github.com/gogo/protobuf/proto"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/internal/common/eid"
)

type eventBuilder struct {
    msg *event.Msg
}

func EventMsg() *eventBuilder {
    aggid := eid.GenerateID()
    t := time.Now().Unix()
    eventID := eid.CreateEventID(aggid, 1)

    return &eventBuilder{
        msg: &event.Msg{
            Id:        eventID,
            EventType: random.SillyName(),
            AggID:     aggid,
            Time:      t,
            Seq:       1,
        },
    }
}

func (b *eventBuilder) Build() *event.Msg {
    return b.msg
}

func (b *eventBuilder) AggregateID(aggid string) *eventBuilder {
    b.msg.AggID = aggid

    return b
}

func (b *eventBuilder) New() *eventBuilder {
    b.Seq(0)

    return b
}

func (b *eventBuilder) Seq(seq int64) *eventBuilder {
    b.msg.Seq = seq
    b.msg.Id = eid.CreateEventID(b.msg.AggID, b.msg.Seq)

    return b
}

func (b *eventBuilder) Time(t int64) *eventBuilder {
    b.msg.Time = t

    return b
}

func (b *eventBuilder) Metadata(metadata event.Metadata) *eventBuilder {
    b.msg.Metadata = metadata

    return b
}

func (b *eventBuilder) Event(evt proto.Message) *eventBuilder {
    data, err := event.MarshalEvent(evt)
    if err != nil {
        panic(err)
    }

    b.msg.Event = data
    b.msg.EventType = proto.MessageName(evt)

    return b
}

func (b *eventBuilder) BuildProto() []byte {
    data, err := event.MarshalMsg(b.msg)
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
