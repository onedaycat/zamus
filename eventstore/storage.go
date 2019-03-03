package eventstore

import (
	"context"

	"github.com/onedaycat/zamus/errors"
)

//go:generate mockery -name=Storage
type Storage interface {
	GetEvents(ctx context.Context, aggID string, seq, limit int64) ([]*EventMsg, errors.Error)
	GetSnapshot(ctx context.Context, aggID string) (*Snapshot, errors.Error)
	Save(ctx context.Context, msgs []*EventMsg, snapshot *Snapshot) errors.Error
}
