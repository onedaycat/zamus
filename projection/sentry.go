package projection

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func PrintPanic(ctx context.Context, msgs EventMsgs, err errors.Error) {
	if err.GetPanic() {
		if cause := err.GetCause(); cause != nil {
			fmt.Println(cause.Error())
		} else if input := err.GetInput(); input != nil {
			fmt.Println(input)
		}
	}
}

func Sentry(ctx context.Context, msgs EventMsgs, err errors.Error) {
	switch err.GetStatus() {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", err.GetInput()).
			Msg(err.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)

	packet.AddError(err)
	sentry.CaptureAndWait(packet)
}
