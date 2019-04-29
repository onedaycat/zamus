package event

import (
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
)

type Metadata = map[string]string
type Event = proto.Message
type Msgs = []*Msg

func (m *Msg) WithEvent(evt Event) {
	m.Event, _ = MarshalEvent(evt)
}

func (m *Msg) UnmarshalEvent(evt proto.Message) errors.Error {
	return UnmarshalEvent(m.Event, evt)
}

func (m *Msg) MustUnmarshalEvent(evt proto.Message) {
	if err := m.UnmarshalEvent(evt); err != nil {
		panic(err)
	}
}

func (m *Msg) MustParseEvent() proto.Message {
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

func MarshalMsg(evt proto.Message) ([]byte, errors.Error) {
	data, err := proto.Marshal(evt)
	if err != nil {
		return nil, appErr.ErrUnableMarshal.WithCause(err).WithCaller().WithInput(evt)
	}

	var dst []byte
	dst = snappy.Encode(dst, data)

	return dst, nil
}

func UnmarshalMsg(data []byte, evt proto.Message) errors.Error {
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

//noinspection GoUnusedExportedFunction
func EventType(evt proto.Message) string {
	return proto.MessageName(evt)
}

//noinspection GoUnusedExportedFunction
func EventTypes(evts ...proto.Message) []string {
	types := make([]string, len(evts))
	for i, evt := range evts {
		types[i] = proto.MessageName(evt)
	}

	return types
}
