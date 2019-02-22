package eventstore

import "encoding/json"

type AggregateMsg struct {
	AggregateID string          `json:"a" bson:"_id"`
	Data        json.RawMessage `json:"d" bson:"d"`
	EventID     string          `json:"i" bson:"i"`
	Time        int64           `json:"t" bson:"t"`
	Seq         int64           `json:"s" bson:"s"`
	TimeSeq     int64           `json:"x" bson:"x"`
}
