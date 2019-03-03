package eventstore

type Snapshot struct {
	AggregateID string `json:"a" bson:"_id"`
	Aggregate   []byte `json:"b" bson:"b"`
	EventID     string `json:"i" bson:"i"`
	Time        int64  `json:"t" bson:"t"`
	Seq         int64  `json:"s" bson:"s"`
}
