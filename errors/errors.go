package errors

import "github.com/onedaycat/errors"

type Input = errors.Input

var (
	ErrQueryResultSizeNotMatch = errors.InternalError("Zamus_ErrQueryResultSizeNotMatch", "Result array size not match")
	ErrUnableParseQuery        = errors.InternalError("Zamus_ErrUnableParseQuery", "Unable to parse query")
	ErrUnablePublishKinesis    = errors.InternalError("Zamus_ErrUnablePublishKinesis", "Unable to publish kinesis stream")
	ErrUnknown                 = errors.InternalError("Zamus_ErrUnknown", "Unknown error")

	ErrVersionInconsistency     = errors.BadRequest("Zamus_ErrVersionInconsistency", "Version is inconsistency")
	ErrEncodingNotSupported     = errors.InternalError("Zamus_ErrEncodingNotSupported", "Unable unmarshal payload, unsupport encoding")
	ErrEventLimitExceed         = errors.InternalError("Zamus_ErrEventLimitExceed", "Number of events in aggregate limit exceed")
	ErrInvalidVersionNotAllowed = errors.InternalError("Zamus_ErrInvalidVersionNotAllowed", "Event sequence should not be 0")
	ErrNoAggregateID            = errors.InternalError("Zamus_ErrNoAggregateID", "No aggregate id in aggregate root")
	ErrUnbleGetEventStore       = errors.InternalError("Zamus_ErrUnbleGetEventStore", "Unable to get")
	ErrUnbleSaveEventStore      = errors.InternalError("Zamus_ErrUnbleSaveEventStore", "Unable to save")
	ErrUnbleSaveDQLMessages     = errors.InternalError("Zamus_ErrUnbleSaveDQLMessages", "Unable to save DQL messages")
)

const (
	ErrPanicType            = "PanicError"
	ErrInternalErrorType    = "InternalError"
	ErrInvalidRequestType   = "InvalidRequest"
	ErrValidateErrorType    = "ValidateError"
	ErrPermissionDeniedType = "PermissionDenied"
	ErrTimeoutType          = "TimeoutError"
	ErrUnauthorizedType     = "Unauthorized"
	ErrUnavailableType      = "Unavailable"
	ErrNotImplementType     = "NotImplement"
	ErrUnableUnmarshalType  = "ErrUnableUnmarshal"
	ErrUnableMarshalType    = "ErrUnableMarshal"
	ErrUnableEncodeType     = "ErrUnableEncode"
	ErrUnableDecodeType     = "ErrUnableDecode"
)

var (
	ErrPanic            = errors.InternalError(ErrPanicType, "Panic Error").WithPanic()
	ErrInternalError    = errors.InternalError(ErrInternalErrorType, "Internal error")
	ErrInvalidRequest   = errors.BadRequest(ErrInvalidRequestType, "Invalid request")
	ErrValidateError    = errors.BadRequest(ErrValidateErrorType, "Validation error")
	ErrPermissionDenied = errors.Forbidden(ErrPermissionDeniedType, "You don't a permission to access this operation")
	ErrTimeout          = errors.Timeout(ErrTimeoutType, "The operation is timeout")
	ErrUnauthorized     = errors.Unauthorized(ErrUnauthorizedType, "The authorization is required")
	ErrUnavailable      = errors.Unavailable(ErrUnavailableType, "This operation is unavailable")
	ErrNotImplement     = errors.NotImplement(ErrNotImplementType, "This operation is not implemented")
	ErrUnableUnmarshal  = errors.InternalError(ErrUnableUnmarshalType, "Unable to unmarshal")
	ErrUnableMarshal    = errors.InternalError(ErrUnableMarshalType, "Unable to marshal")
	ErrUnableEncode     = errors.InternalError(ErrUnableEncodeType, "Unable to encode")
	ErrUnableDecode     = errors.InternalError(ErrUnableDecodeType, "Unable to decode")
)

func ErrCommandNotFound(command string) errors.Error {
	return errors.BadRequestf("ZAMUS_ErrCommandNotFound", "%s command not found", command)
}

func ErrQueryNotFound(query string) errors.Error {
	return errors.BadRequestf("ZAMUS_ErrQueryNotFound", "%s query not found", query)
}
