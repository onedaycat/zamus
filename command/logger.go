package command

import (
	"context"
	"encoding/json"

	"github.com/onedaycat/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func ErrorLog(ctx context.Context, cmd *Command, err errors.Error) {
	var cause string
	var input []byte

	level := zerolog.ErrorLevel

	if c := err.GetCause(); c != nil {
		cause = c.Error()
	}

	if in := err.GetInput(); in != nil {
		input, _ = json.Marshal(in)
	}

	if err.GetPanic() {
		level = zerolog.PanicLevel
	}

	log.WithLevel(level).
		Bytes("input", input).
		Str("cause", cause).
		Msg(err.Error())
}