package eventstore

type Snapshot struct {
    AggregateID string `json:"aggregateID"`
    Aggregate   []byte `json:"aggregate"`
    EventID     string `json:"eventID"`
    Time        int64  `json:"time"`
    Seq         int64  `json:"seq"`
    Version     int    `json:"version"`
}
