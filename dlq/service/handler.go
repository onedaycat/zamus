package service

import (
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
)

var (
    ErrInvalidRequest = errors.DefBadRequest("ErrInvalidRequest", "Invalid request")
    ErrInternalError  = errors.DefInternalError("ErrInternalError ", "Internal error")
)

type handler struct {
    storage dlq.Storage
}

func NewHandler(storage dlq.Storage) *handler {
    return &handler{
        storage: storage,
    }
}
