package eventstore

import "encoding/json"

type SnapshotMsg struct {
	AggregateID string          `json:"a" bson:"_id"`
	HashKey     string          `json:"h" bson:"h"`
	Data        json.RawMessage `json:"p" bson:"p"`
	EventID     string          `json:"i" bson:"i"`
	Time        int64           `json:"t" bson:"t"`
	Seq         int64           `json:"s" bson:"s"`
	TimeSeq     int64           `json:"x" bson:"x"`
}

type SnapshotStategyHandler func(agg AggregateRoot, events []*EventMsg) bool

func EveryNEventSanpshot(nEvent int64) SnapshotStategyHandler {
	return func(agg AggregateRoot, events []*EventMsg) bool {
		for _, event := range events {
			if event.Seq%nEvent == 0 {
				return true
			}
		}

		return false
	}
}

func LatestEventSanpshot() SnapshotStategyHandler {
	return func(agg AggregateRoot, events []*EventMsg) bool {
		return true
	}
}
