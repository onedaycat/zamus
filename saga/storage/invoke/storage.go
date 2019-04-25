package invoke

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/invoke"
    "github.com/onedaycat/zamus/saga"
    "github.com/onedaycat/zamus/saga/service"
)

var (
    ErrUnableSaveSaga = errors.DefInternalError("ErrUnableSaveSaga", "Unable to save saga")
    ErrUnableGetSaga  = errors.DefInternalError("ErrUnableGetSaga", "Unable to get saga")
)

type invokeService struct {
    invoker invoke.Invoker
    fn      string
}

func New(invoker invoke.Invoker, fn string) *invokeService {
    return &invokeService{
        invoker: invoker,
        fn:      fn,
    }
}

func (s *invokeService) Get(ctx context.Context, stateName, id string) (*saga.State, errors.Error) {
    input := &service.GetStateInput{
        ID:        id,
        StateName: stateName,
    }

    req := invoke.NewRequest(service.GetStateMethod).WithInput(input)

    output := &service.GetStateOuput{}

    if err := s.invoker.Invoke(ctx, s.fn, req, output); err != nil {
        return nil, ErrUnableGetSaga.WithCause(err).WithCaller().WithInput(input)
    }

    return output.State, nil
}

func (s *invokeService) Save(ctx context.Context, state *saga.State) errors.Error {
    input := &service.SaveStateInput{
        State: state,
    }

    req := invoke.NewRequest(service.SaveStateMethod).WithInput(input)
    if err := s.invoker.Invoke(ctx, s.fn, req, nil); err != nil {
        return ErrUnableSaveSaga.WithCause(err).WithCaller().WithInput(input)
    }

    return nil
}
