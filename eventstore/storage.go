package eventstore

//go:generate mockery -name=Storage
// Get(id string, withSnapshot bool)
type Storage interface {
	GetEvents(aggID string, seq, limit int64) ([]*EventMsg, error)
	GetSnapshot(aggID string) (*Snapshot, error)
	Save(msgs []*EventMsg, snapshot *Snapshot) error
}
