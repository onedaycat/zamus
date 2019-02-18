package query

import "github.com/onedaycat/errors"

var (
	ErrPermissionDenied        = errors.Forbidden("QUERY_FORBIDDEN", "You don't a permission to access this operation")
	ErrQueryResultSizeNotMatch = errors.InternalError("QUERY_MISMATCH_RESULT", "Result array size not match")
	ErrUnableParseQuery        = errors.InternalError("QUERY_PARSE_QUERY", "Unable to parse query")
)

func ErrQueryNotFound(query string) error {
	return errors.BadRequestf("QUERY_NOT_FOUND", "%s query not found", query)
}

func makeError(err error) error {
	_, ok := errors.FromError(err)
	if ok {
		return err
	}

	return errors.InternalError("UNKNOWN_QUERY", err.Error())
}
