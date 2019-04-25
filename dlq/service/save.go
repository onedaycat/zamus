package service

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/service"
)

const (
    SaveDLQMsgMethod = "SaveDLQMsg"
)

type SaveDLQMsgInput struct {
    Msg *dlq.DLQMsg `json:"msg"`
}

func (h *handler) SaveDLQMsg(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
    input := &SaveDLQMsgInput{}
    if err := req.ParseInput(input); err != nil {
        return nil, ErrInvalidRequest.New()
    }

    if err := h.storage.Save(ctx, input.Msg); err != nil {
        return nil, ErrInternalError.WithCaller().WithInput(input).WithCause(err)
    }

    return input.Msg.ID, nil
}
