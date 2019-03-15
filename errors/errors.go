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

var (
	ErrPanic            = errors.InternalError("PanicError", "Panic Error").WithPanic()
	ErrInternalError    = errors.InternalError("InternalError", "Internal error")
	ErrInvalidRequest   = errors.BadRequest("InvalidRequest", "Invalid request")
	ErrValidateError    = errors.BadRequest("ValidateError", "Validation error")
	ErrPermissionDenied = errors.Forbidden("PermissionDenied", "You don't a permission to access this operation")
	ErrTimeout          = errors.Timeout("TimeoutError", "The operation is timeout")
	ErrUnauthorized     = errors.Unauthorized("Unauthorized", "The authorization is required")
	ErrUnavailable      = errors.Unavailable("Unavailable", "This operation is unavailable")
	ErrNotImplement     = errors.NotImplement("NotImplement", "This operation is not implemented")
	ErrUnableUnmarshal  = errors.InternalError("ErrUnableUnmarshal", "Unable to unmarshal")
	ErrUnableMarshal    = errors.InternalError("ErrUnableMarshal", "Unable to marshal")
	ErrUnableEncode     = errors.InternalError("ErrUnableEncode", "Unable to encode")
	ErrUnableDecode     = errors.InternalError("ErrUnableDecode", "Unable to decode")
)

func ErrCommandNotFound(command string) errors.Error {
	return errors.BadRequestf("ZAMUS_ErrCommandNotFound", "%s command not found", command)
}

func ErrQueryNotFound(query string) errors.Error {
	return errors.BadRequestf("ZAMUS_ErrQueryNotFound", "%s query not found", query)
}
