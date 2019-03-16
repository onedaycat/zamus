package command

import (
	"context"

	"github.com/onedaycat/zamus/common"

	"github.com/onedaycat/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func ErrorLog(ctx context.Context, req *CommandReq, err errors.Error) {
	var cause string
	var input []byte

	level := zerolog.ErrorLevel

	if c := err.GetCause(); c != nil {
		cause = c.Error()
	}

	if in := err.GetInput(); in != nil {
		input, _ = common.MarshalJSON(in)
	}

	if err.GetPanic() {
		level = zerolog.PanicLevel
	}

	log.WithLevel(level).
		Bytes("input", input).
		Str("cause", cause).
		Msg(err.Error())
}
