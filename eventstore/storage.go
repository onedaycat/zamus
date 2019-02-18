package eventstore

//go:generate mockery -name=Storage
// Get(id string, withSnapshot bool)
type Storage interface {
	GetEvents(aggID, hashKey string, seq, limit int64) ([]*EventMsg, error)
	GetEventsByEventType(eventType string, seq, limit int64) ([]*EventMsg, error)
	GetEventsByAggregateType(aggType string, seq, limit int64) ([]*EventMsg, error)
	GetAggregate(aggID, hashKey string) (*AggregateMsg, error)
	GetSnapshot(aggID, hashKey string) (*SnapshotMsg, error)
	Save(events []*EventMsg, agg *AggregateMsg) error
}
