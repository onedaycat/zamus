package eventstore

import "encoding/json"

type Metadata = map[string]string

func (e *EventMsg) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(e.Data, v)
}

// EventID              string    `protobuf:"bytes,1,opt,name=eventID,proto3" json:"i,omitempty"`
// EventType            string    `protobuf:"bytes,2,opt,name=eventType,proto3" json:"e,omitempty"`
// AggregateID          string    `protobuf:"bytes,3,opt,name=aggregateID,proto3" json:"a,omitempty"`
// AggregateType        string    `protobuf:"bytes,4,opt,name=aggregateType,proto3" json:"b,omitempty"`
// PartitionKey         string    `protobuf:"bytes,5,opt,name=partitionKey,proto3" json:"k,omitempty"`
// HashKey              string    `protobuf:"bytes,6,opt,name=hashKey,proto3" json:"h,omitempty"`
// Data                 []byte    `protobuf:"bytes,7,opt,name=data,proto3" json:"d,omitempty"`
// Time                 int64     `protobuf:"varint,8,opt,name=time,proto3" json:"t,omitempty"`
// Seq                  int64     `protobuf:"varint,9,opt,name=seq,proto3" json:"s,omitempty"`
// TimeSeq              int64     `protobuf:"varint,10,opt,name=timeSeq,proto3" json:"x,omitempty"`
// UserID               string   `protobuf:"bytes,11,opt,name=userID,proto3" json:"u,omitempty"`
