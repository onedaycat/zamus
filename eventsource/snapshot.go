package eventsource

type Snapshot struct {
	AggID      string `json:"aggID"`
	Agg        []byte `json:"agg"`
	EventMsgID string `json:"eventMsgID"`
	Time       int64  `json:"time"`
	Seq        int64  `json:"seq"`
	Version    int    `json:"version"`
}
