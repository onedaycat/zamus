package trigger

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, id string, err errors.Error) {
	switch errors.ErrStatus(err) {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", err.GetInput()).
			Msg(err.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)
	if id != "" {
		packet.AddExtra(sentry.Extra{
			"id": id,
		})
	}

	packet.AddError(err)
	sentry.CaptureAndWait(packet)
}
