package saga

import (
    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
)

type Request struct {
    Input  jsoniter.RawMessage `json:"input"`
    Resume string              `json:"resume"`
}

func (r *Request) clear() {
    r.Input = nil
    r.Resume = emptyStr
}

type Response struct {
    Success bool             `json:"success"`
    Error   *errors.AppError `json:"error"`
}
