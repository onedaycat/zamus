package eventstore

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
)

//go:generate mockery -name=Storage
type Storage interface {
	Save(ctx context.Context, msgs event.Msgs) errors.Error
}
