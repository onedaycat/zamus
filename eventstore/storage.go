package eventstore

import "context"

//go:generate mockery -name=Storage
// Get(id string, withSnapshot bool)
type Storage interface {
	GetEvents(ctx context.Context, aggID string, seq, limit int64) ([]*EventMsg, error)
	GetSnapshot(ctx context.Context, aggID string) (*Snapshot, error)
	Save(ctx context.Context, msgs []*EventMsg, snapshot *Snapshot) error
}
