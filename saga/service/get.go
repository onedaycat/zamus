package service

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/saga"
    "github.com/onedaycat/zamus/service"
)

const (
    GetStateMethod = "GetState"
)

type GetStateInput struct {
    ID        string `json:"id"`
    StateName string `json:"stateName"`
}

type GetStateOuput struct {
    State *saga.State `json:"state"`
}

func (h *handler) GetState(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
    input := &GetStateInput{}
    if err := req.ParseInput(input); err != nil {
        return nil, ErrInvalidRequest.New()
    }

    state := &saga.State{}
    err := h.storage.Get(ctx, input.ID, state)
    if err != nil {
        return nil, ErrInternalError.WithCaller().WithInput(input).WithCause(err)
    }

    return &GetStateOuput{
        State: state,
    }, nil
}
