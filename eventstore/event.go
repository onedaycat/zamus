package eventstore

import (
	"encoding/json"
	"time"

	"github.com/golang/snappy"
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
)

func (e *EventMsg) UnmarshalEvent(v interface{}) errors.Error {
	var dst []byte
	var err error
	dst, err = snappy.Decode(dst, e.Event)
	if err != nil {
		return appErr.ErrUnableDecode.WithCause(err).WithCaller()
	}

	if err := json.Unmarshal(dst, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *EventMsg) AddExpired(d time.Duration) {
	e.Expired = time.Unix(e.Time, 0).Add(d).Unix()
}

func (e *EventMsg) UnmarshalMetadata() *Metadata {
	metadata := &Metadata{}
	if e.Metadata == nil {
		return metadata
	}

	metadata.Unmarshal(e.Metadata)

	return metadata
}

// EventID              string   `protobuf:"bytes,1,opt,name=eventID,proto3" json:"i,omitempty"`
// EventType            string   `protobuf:"bytes,2,opt,name=eventType,proto3" json:"b,omitempty"`
// EventVersion         string   `protobuf:"bytes,3,opt,name=eventVersion,proto3" json:"v,omitempty"`
// AggregateID          string   `protobuf:"bytes,4,opt,name=aggregateID,proto3" json:"a,omitempty"`
// Event                []byte   `protobuf:"bytes,5,opt,name=event,proto3" json:"e,omitempty"`
// Time                 int64    `protobuf:"varint,6,opt,name=time,proto3" json:"t,omitempty"`
// Seq                  int64    `protobuf:"varint,7,opt,name=seq,proto3" json:"s,omitempty"`
// Expired              int64    `protobuf:"varint,8,opt,name=expired,proto3" json:"l,omitempty"`
// Metadata             []byte   `protobuf:"bytes,9,opt,name=metadata,proto3" json:"m,omitempty"`

// EventMsgs            []*EventMsg `protobuf:"bytes,1,rep,name=eventMsgs,proto3" json:"e,omitempty"`

// Extra                map[string]string `protobuf:"bytes,1,rep,name=extra,proto3" json:"e,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
// UserID               string            `protobuf:"bytes,2,opt,name=userID,proto3" json:"u,omitempty"`
// Ip                   string            `protobuf:"bytes,3,opt,name=ip,proto3" json:"i,omitempty"`
// CorrelationID        string            `protobuf:"bytes,4,opt,name=correlationID,proto3" json:"c,omitempty"`
// TxID                 string            `protobuf:"bytes,5,opt,name=txID,proto3" json:"t,omitempty"`
