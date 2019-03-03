package dql

import "context"

//go:generate mockery -name=Storage
type Storage interface {
	MultiSave(ctx context.Context, msgs DQLMsgs) error
}
