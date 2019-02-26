package errors

import "github.com/onedaycat/errors"

type Input = errors.Input

var (
	ErrPermissionDenied        = errors.Forbidden("COMMAND_FORBIDDEN", "You don't a permission to access this operation")
	ErrQueryResultSizeNotMatch = errors.InternalError("QUERY_MISMATCH_RESULT", "Result array size not match")
	ErrUnableParseQuery        = errors.InternalError("QUERY_PARSE_QUERY", "Unable to parse query")
	ErrPanic                   = errors.InternalError("PANIC", "Server Error")
	ErrUnknown                 = errors.InternalError("UNKNOWN_COMMAND", "Unknown error")

	ErrNotFound                 = errors.NotFound("es1", "Not Found")
	ErrVersionInconsistency     = errors.BadRequest("es2", "Version is inconsistency")
	ErrEncodingNotSupported     = errors.InternalError("es3", "Unable unmarshal payload, unsupport encoding")
	ErrEventLimitExceed         = errors.InternalError("es4", "Number of events in aggregate limit exceed")
	ErrInvalidVersionNotAllowed = errors.InternalError("es5", "Event sequence should not be 0")
	ErrNoAggregateID            = errors.InternalError("es6", "No aggregate id in aggregate root")
	ErrUnbleGetEventStore       = errors.InternalError("es7", "Unable to get")
	ErrUnbleSaveEventStore      = errors.InternalError("es8", "Unable to save")
)

func ErrCommandNotFound(command string) error {
	return errors.BadRequestf("COMMAND_NOT_FOUND", "%s command not found", command)
}

func ErrQueryNotFound(query string) error {
	return errors.BadRequestf("QUERY_NOT_FOUND", "%s query not found", query)
}

func Warp(err error) *errors.AppError {
	appErr, ok := errors.FromError(err)
	if ok {
		return appErr
	}

	return ErrUnknown.WithCause(err)
}
