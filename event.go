package gocqrs

import "encoding/json"

func (e *EventMessage) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(e.Payload, v)
}

// go1:generate msgp -file payload.go
// go1:generate msgp

// type EventMessage struct {
// 	EventID       string            `json:"i" msg:"i" bson:"_id"`
// 	HashKey       string            `json:"h" msg:"h" bson:"h"` // sharind of event store
// 	PartitionKey  string            `json:"k" msg:"k" bson:"k"` // sharding of event bus
// 	AggregateID   string            `json:"a" msg:"a" bson:"a"`
// 	AggregateType string            `json:"b" msg:"b" bson:"b"`
// 	EventType     string            `json:"e" msg:"e" bson:"e"`
// 	Payload       RawMessage        `json:"p" msg:"p" bson:"p"`
// 	Time          int64             `json:"t" msg:"t" bson:"t"`
// 	Seq           int64             `json:"s" msg:"s" bson:"s"`
// 	TimeSeq       int64             `json:"x" msg:"x" bson:"x"`
// 	Metadata      map[string]string `json:"m" msg:"m" bson:"m"`
// }
