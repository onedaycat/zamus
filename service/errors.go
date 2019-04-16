package service

import (
    appErr "github.com/onedaycat/zamus/errors"
)

//noinspection GoUnusedGlobalVariable
var (
    ErrInternalError    = appErr.ErrInternalError
    ErrInvalidRequest   = appErr.ErrInvalidRequest
    ErrValidateError    = appErr.ErrValidateError
    ErrPermissionDenied = appErr.ErrPermissionDenied
    ErrTimeout          = appErr.ErrTimeout
    ErrUnauthorized     = appErr.ErrUnauthorized
    ErrUnavailable      = appErr.ErrUnavailable
    ErrNotImplement     = appErr.ErrNotImplement
    ErrUnableUnmarshal  = appErr.ErrUnableUnmarshal
    ErrUnableMarshal    = appErr.ErrUnableMarshal
    ErrUnableEncode     = appErr.ErrUnableEncode
    ErrUnableDecode     = appErr.ErrUnableDecode
    ErrUnableApplyEvent = appErr.ErrUnableApplyEvent
)
