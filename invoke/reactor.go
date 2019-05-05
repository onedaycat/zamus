package invoke

import (
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
)

type ReactorRequest struct {
    Msgs []byte `json:"msgs"`
    fn   string
}

func NewReactorRequest(fn string) *ReactorRequest {
    return &ReactorRequest{
        fn: fn,
    }
}

func (e *ReactorRequest) WithEventList(msgList *event.MsgList) *ReactorRequest {
    e.Msgs, _ = event.MarshalMsg(msgList)

    return e
}

func (e *ReactorRequest) MarshalRequest() ([]byte, errors.Error) {
    return common.MarshalJSON(e)
}
