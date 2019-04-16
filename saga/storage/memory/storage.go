package memory

import (
    "context"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/saga"
)

const delim = "_"

type SagaStorage struct {
    data map[string]*saga.State
}

func New() *SagaStorage {
    return &SagaStorage{
        data: make(map[string]*saga.State),
    }
}

func (s *SagaStorage) Clear() {
    s.data = make(map[string]*saga.State)
}

func (s *SagaStorage) Get(ctx context.Context, stateName, id string) (*saga.State, errors.Error) {
    state, ok := s.data[stateName+delim+id]
    if !ok {
        return nil, appErr.ErrStateNotFound.WithCaller().WithInput(stateName + delim + id)
    }

    return state, nil
}

func (s *SagaStorage) Save(ctx context.Context, state *saga.State) errors.Error {
    s.data[state.Name+delim+state.ID] = state

    return nil
}
