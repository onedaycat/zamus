package service

import (
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/saga"
)

var (
    ErrInvalidRequest = errors.DefBadRequest("ErrInvalidRequest", "Invalid request")
    ErrInternalError  = errors.DefInternalError("ErrInternalError ", "Internal error")
)

type handler struct {
    storage saga.Storage
}

func NewHandler(storage saga.Storage) *handler {
    return &handler{
        storage: storage,
    }
}
