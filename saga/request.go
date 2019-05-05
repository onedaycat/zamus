package saga

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
)

type Request struct {
    EventMsg *event.Msg `json:"eventMsg"`
    Resume   string     `json:"resume"`
}

func (r *Request) clear() {
    r.EventMsg = nil
    r.Resume = emptyStr
}

type Response struct {
    Success bool             `json:"success"`
    Error   *errors.AppError `json:"error"`
}

type Source interface {
    GetRequest(ctx context.Context, payload []byte) ([]*Request, errors.Error)
}
