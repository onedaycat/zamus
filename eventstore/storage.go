package eventstore

//go:generate mockery -name=Storage
// Get(id string, withSnapshot bool)
type Storage interface {
	GetEvents(aggID string, seq, limit int64) ([]*EventMsg, error)
	GetEventsByEventType(eventType string, seq, limit int64) ([]*EventMsg, error)
	GetEventsByAggregateType(aggType string, seq, limit int64) ([]*EventMsg, error)
	GetAggregate(aggID string) (*AggregateMsg, error)
	GetSnapshot(aggID string) (*SnapshotMsg, error)
	Save(events []*EventMsg, agg *AggregateMsg) error
}
