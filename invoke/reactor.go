package invoke

import (
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
)

type ReactorRequest struct {
    MsgList *event.MsgList `json:"msgList,omitempty"`
    fn      string
}

func NewReactorRequest(fn string) *ReactorRequest {
    return &ReactorRequest{
        fn: fn,
    }
}

func (e *ReactorRequest) WithEventList(msgList *event.MsgList) *ReactorRequest {
    e.MsgList = msgList

    return e
}

func (e *ReactorRequest) MarshalRequest() ([]byte, errors.Error) {
    return event.MarshalMsg(e.MsgList)
}
