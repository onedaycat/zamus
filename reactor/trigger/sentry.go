package trigger

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, id string, err error) {
	var appErr *errors.AppError

	appErr, _ = errors.FromError(err)
	switch errors.ErrStatus(err) {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", appErr.Input).
			Msg(appErr.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)
	if id != "" {
		packet.AddExtra(sentry.Extra{
			"id": id,
		})
	}

	if appErr.Input != nil {
		packet.AddExtra(sentry.Extra{
			"input": appErr.Input,
		})
	}

	if appErr.Cause != nil {
		packet.AddExtra(sentry.Extra{
			"cause": appErr.Cause.Error(),
		})
	}

	packet.AddStackTrace(appErr.StackTrace())
	sentry.CaptureAndWait(packet)
}
