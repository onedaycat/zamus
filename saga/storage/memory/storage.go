package memory

import (
	"context"

	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/saga"
)

type MemoryStorage struct {
	data map[string]*saga.State
}

func New() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string]*saga.State),
	}
}

func (s *MemoryStorage) Clear() {
	s.data = make(map[string]*saga.State)
}

func (s *MemoryStorage) Get(ctx context.Context, id string) (*saga.State, errors.Error) {
	state, ok := s.data[id]
	if !ok {
		return nil, appErr.ErrStateNotFound(id).WithCaller().WithInput(id)
	}

	return state, nil
}

func (s *MemoryStorage) Save(ctx context.Context, state *saga.State) errors.Error {
	s.data[state.ID] = state

	return nil
}
