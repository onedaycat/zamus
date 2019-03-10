package eventstore

import (
	"context"

	"github.com/onedaycat/errors"
)

//go:generate mockery -name=Storage
type Storage interface {
	GetEvents(ctx context.Context, aggID string, seq int64) ([]*EventMsg, errors.Error)
	GetSnapshot(ctx context.Context, aggID string, version int) (*Snapshot, errors.Error)
	Save(ctx context.Context, msgs []*EventMsg, snapshot *Snapshot) errors.Error
}
