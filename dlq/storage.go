package dlq

import (
    "context"

    "github.com/onedaycat/errors"
)

//go:generate mockery -name=Storage
type Storage interface {
    Save(ctx context.Context, dlq *DLQMsg) errors.Error
    Get(ctx context.Context, lambdaType LambdaType, id string) (*DLQMsg, errors.Error)
}
