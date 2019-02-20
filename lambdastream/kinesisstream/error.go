package kinesisstream

import "github.com/onedaycat/errors"

func makeError(err error) error {
	_, ok := errors.FromError(err)
	if ok {
		return err
	}

	return errors.InternalError("UNKNOWN", err.Error())
}
