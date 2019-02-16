package gocqrs

//go:generate mockery -name=Storage
// Get(id string, withSnapshot bool)
type Storage interface {
	GetEvents(aggID, hashKey string, seq, limit int64) ([]*EventMessage, error)
	GetEventsByEventType(eventType EventType, seq, limit int64) ([]*EventMessage, error)
	GetEventsByAggregateType(aggType AggregateType, seq, limit int64) ([]*EventMessage, error)
	GetSnapshot(aggID, hashKey string) (*Snapshot, error)
	Save(events []*EventMessage, snapshot *Snapshot) error
}
