package invoke

import (
	"github.com/onedaycat/errors"
)

var (
	ErrBatchInvokeResultSizeNotMatch = errors.InternalError("AMURO_1", "BatchInvoke result array size not match")
	ErrNoResult                      = errors.InternalError("AMURO_2", "No Result")
	ErrNoBatchInvokeData             = errors.InternalError("AMURO_3", "No data in batch invoke")
)

func ErrFuncNotFound(function string) error {
	return errors.InternalErrorf("INVOKE_FUNC_NOT_FOUND", "Not found handler on function %s", function)
}

func makeError(err error) error {
	_, ok := errors.FromError(err)
	if ok {
		return err
	}

	return errors.InternalError("AMURO_UNKNOWN", err.Error())
}
