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

	if err.GetInput() != nil {
		packet.AddExtra(sentry.Extra{
			"input": err.GetInput(),
		})
	}

	packet.AddError(err)
	sentry.CaptureAndWait(packet)
}
