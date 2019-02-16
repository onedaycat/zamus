package gocqrs

import "encoding/json"

type Snapshot struct {
	AggregateID   string          `json:"a" bson:"_id"`
	AggregateType AggregateType   `json:"b" bson:"b"`
	HashKey       string          `json:"h" bson:"h"`
	Payload       json.RawMessage `json:"p" bson:"p"`
	EventID       string          `json:"i" bson:"i"`
	Time          int64           `json:"t" bson:"t"`
	Seq           int64           `json:"s" bson:"s"`
	TimeSeq       int64           `json:"x" bson:"x"`
	Metadata      *Metadata       `json:"m" bson:"m"`
}

type SnapshotStategyHandler func(agg AggregateRoot, events []*EventMessage) bool

func EveryNEventSanpshot(nEvent int64) SnapshotStategyHandler {
	return func(agg AggregateRoot, events []*EventMessage) bool {
		for _, event := range events {
			if event.Seq%nEvent == 0 {
				return true
			}
		}

		return false
	}
}

func LatestEventSanpshot() SnapshotStategyHandler {
	return func(agg AggregateRoot, events []*EventMessage) bool {
		return true
	}
}
