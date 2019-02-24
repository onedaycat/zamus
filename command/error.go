package command

import "github.com/onedaycat/errors"

var (
	ErrPermissionDenied = errors.Forbidden("COMMAND_FORBIDDEN", "You don't a permission to access this operation")
)

func ErrCommandNotFound(command string) error {
	return errors.BadRequestf("COMMAND_NOT_FOUND", "%s command not found", command)
}

func makeError(err error) error {
	_, ok := errors.FromError(err)
	if ok {
		return err
	}

	return errors.InternalError("UNKNOWN_COMMAND", "Unknown error").WithCause(err).WithCaller()
}
