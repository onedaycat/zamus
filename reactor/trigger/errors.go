package trigger

import "github.com/onedaycat/zamus/errors"

var (
	ErrInternalError    = errors.ErrInternalError
	ErrInvalidRequest   = errors.ErrInvalidRequest
	ErrValidateError    = errors.ErrValidateError
	ErrPermissionDenied = errors.ErrPermissionDenied
	ErrTimeout          = errors.ErrTimeout
	ErrUnauthorized     = errors.ErrUnauthorized
	ErrUnavailable      = errors.ErrUnavailable
	ErrNotImplement     = errors.ErrNotImplement
	ErrUnableUnmarshal  = errors.ErrUnableUnmarshal
	ErrUnableMarshal    = errors.ErrUnableMarshal
	ErrUnableEncode     = errors.ErrUnableEncode
	ErrUnableDecode     = errors.ErrUnableDecode
)
