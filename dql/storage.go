package dql

import "context"

//go:generate mockery -name=Storage
type Storage interface {
	Save(ctx context.Context, dqlMsg *DQLMsg) error
}
