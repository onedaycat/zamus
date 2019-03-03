package dql

import (
	"context"

	"github.com/onedaycat/errors"
)

//go:generate mockery -name=Storage
type Storage interface {
	Save(ctx context.Context, dqlMsg *DQLMsg) errors.Error
}
