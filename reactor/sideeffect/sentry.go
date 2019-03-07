package sideeffect

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, msgs EventMsgs, err errors.Error) {
	switch errors.ErrStatus(err) {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", err.GetInput()).
			Msg(err.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)

	if err.GetInput() != nil {
		packet.AddExtra(sentry.Extra{
			"input": err.GetInput(),
		})
	}

	packet.AddError(err)
	sentry.CaptureAndWait(packet)
}
