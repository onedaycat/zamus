package errors

import "github.com/onedaycat/errors"

type Input = errors.Input

type Error = errors.Error

var (
	ErrPermissionDenied        = errors.Forbidden("Zamus_ErrPermissionDenied", "You don't a permission to access this operation")
	ErrQueryResultSizeNotMatch = errors.InternalError("Zamus_ErrQueryResultSizeNotMatch", "Result array size not match")
	ErrUnableParseQuery        = errors.InternalError("Zamus_ErrUnableParseQuery", "Unable to parse query")
	ErrUnableUnmarshal         = errors.InternalError("Zamus_ErrUnableUnmarshal", "Unable to unmarshal")
	ErrUnableMarshal           = errors.InternalError("Zamus_ErrUnableMarshal", "Unable to marshal")
	ErrUnableEncode            = errors.InternalError("Zamus_ErrUnableEncode", "Unable to encode")
	ErrUnableDecode            = errors.InternalError("Zamus_ErrUnableDecode", "Unable to decode")
	ErrUnablePublishKinesis    = errors.InternalError("Zamus_ErrUnablePublishKinesis", "Unable to publish kinesis stream")
	ErrPanic                   = errors.InternalError("Zamus_ErrPanic", "Server Error")
	ErrUnknown                 = errors.InternalError("Zamus_ErrUnknown", "Unknown error")

	ErrNotFound                 = errors.NotFound("Zamus_ErrNotFound", "Not Found")
	ErrVersionInconsistency     = errors.BadRequest("Zamus_ErrVersionInconsistency", "Version is inconsistency")
	ErrEncodingNotSupported     = errors.InternalError("Zamus_ErrEncodingNotSupported", "Unable unmarshal payload, unsupport encoding")
	ErrEventLimitExceed         = errors.InternalError("Zamus_ErrEventLimitExceed", "Number of events in aggregate limit exceed")
	ErrInvalidVersionNotAllowed = errors.InternalError("Zamus_ErrInvalidVersionNotAllowed", "Event sequence should not be 0")
	ErrNoAggregateID            = errors.InternalError("Zamus_ErrNoAggregateID", "No aggregate id in aggregate root")
	ErrUnbleGetEventStore       = errors.InternalError("Zamus_ErrUnbleGetEventStore", "Unable to get")
	ErrUnbleSaveEventStore      = errors.InternalError("Zamus_ErrUnbleSaveEventStore", "Unable to save")
	ErrUnbleSaveDQLMessages     = errors.InternalError("Zamus_ErrUnbleSaveDQLMessages", "Unable to save DQL messages")
)

func ErrCommandNotFound(command string) errors.Error {
	return errors.BadRequestf("ZAMUS_ErrCommandNotFound", "%s command not found", command)
}

func ErrQueryNotFound(query string) errors.Error {
	return errors.BadRequestf("ZAMUS_ErrQueryNotFound", "%s query not found", query)
}

func Wrap(err error) errors.Error {
	appErr, ok := errors.FromError(err)
	if ok {
		return appErr
	}

	return ErrUnknown.WithCause(err)
}
