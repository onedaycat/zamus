package saga

import (
    "context"

    "github.com/onedaycat/errors"
)

//go:generate mockery -name=Storage
type Storage interface {
    Get(ctx context.Context, stateName, id string) (*State, errors.Error)
    Save(ctx context.Context, state *State) errors.Error
}
