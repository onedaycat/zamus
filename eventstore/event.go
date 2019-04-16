package eventstore

import (
    "reflect"
    "time"

    "github.com/gogo/protobuf/proto"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/common"
    appErr "github.com/onedaycat/zamus/errors"
)

type EventPublish struct {
    Event    proto.Message
    Metadata Metadata
    // optional, auto id if empty
    AggregateID string
    // optional, auto seq if 0
    Seq int64
}

func (m *EventMsg) UnmarshalEvent(evt proto.Message) errors.Error {
    return common.UnmarshalEvent(m.Event, evt)
}

func (m *EventMsg) MustUnmarshalEvent(evt proto.Message) {
    if err := m.UnmarshalEvent(evt); err != nil {
        panic(err)
    }
}

func (m *EventMsg) AddExpired(d time.Duration) {
    m.Expired = time.Unix(m.Time, 0).Add(d).Unix()
}

func (m *EventMsg) MustParseEvent() proto.Message {
    t := proto.MessageType(m.EventType)
    if t == nil {
        panic(appErr.ErrEventProtoNotRegistered.WithCaller().WithInput(m))
    }
    msg := reflect.New(t.Elem()).Interface().(proto.Message)

    if err := common.UnmarshalEvent(m.Event, msg); err != nil {
        panic(err)
    }

    return msg
}
