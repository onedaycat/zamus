package eventsource

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
)

//go:generate mockery -name=Storage
type Storage interface {
	GetEvents(ctx context.Context, aggID string, seq int64) (event.Msgs, errors.Error)
	GetSnapshot(ctx context.Context, aggID string, version int) (*Snapshot, errors.Error)
	Save(ctx context.Context, msgs event.Msgs, snapshot *Snapshot) errors.Error
}
