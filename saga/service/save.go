package service

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/saga"
    "github.com/onedaycat/zamus/service"
)

const (
    SaveStateMethod = "SaveState"
)

type SaveStateInput struct {
    State *saga.State `json:"state"`
}

func (h *handler) SaveState(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
    input := &SaveStateInput{}
    if err := req.ParseInput(input); err != nil {
        return nil, ErrInvalidRequest.New()
    }

    if err := h.storage.Save(ctx, input.State); err != nil {
        return nil, ErrInternalError.WithCaller().WithInput(input).WithCause(err)
    }

    return input.State.ID, nil
}
