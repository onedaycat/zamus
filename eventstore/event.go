package eventstore

import (
    "reflect"
    "time"

    "github.com/gogo/protobuf/proto"
    "github.com/golang/snappy"
    "github.com/onedaycat/errors"
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
    return UnmarshalEvent(m.Event, evt)
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

    if err := UnmarshalEvent(m.Event, msg); err != nil {
        panic(err)
    }

    return msg
}

func MarshalEvent(evt proto.Message) ([]byte, errors.Error) {
    data, err := proto.Marshal(evt)
    if err != nil {
        return nil, appErr.ErrUnableMarshal.WithCause(err).WithCaller().WithInput(evt)
    }

    if len(data) == 0 {
        return nil, nil
    }

    return data, nil
}

func UnmarshalEvent(data []byte, evt proto.Message) errors.Error {
    err := proto.Unmarshal(data, evt)
    if err != nil {
        return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(evt)
    }

    return nil
}

func MarshalEventMsg(evt proto.Message) ([]byte, errors.Error) {
    data, err := proto.Marshal(evt)
    if err != nil {
        return nil, appErr.ErrUnableMarshal.WithCause(err).WithCaller().WithInput(evt)
    }

    var dst []byte
    dst = snappy.Encode(dst, data)

    return dst, nil
}

func UnmarshalEventMsg(data []byte, evt proto.Message) errors.Error {
    var dst []byte
    var err error
    dst, err = snappy.Decode(dst, data)
    if err != nil {
        return appErr.ErrUnableDecode.WithCause(err).WithCaller().WithInput(data)
    }

    if err := proto.Unmarshal(dst, evt); err != nil {
        return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(dst)
    }

    return nil
}
