package command

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, cmd *Command, err errors.Error) {
	switch errors.ErrStatus(err) {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", err.GetInput()).
			Msg(err.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)
	if cmd.Identity != nil && cmd.Identity.GetID() != "" {
		packet.AddUser(&sentry.User{
			ID: cmd.Identity.GetID(),
		})
	}

	if err.GetInput() != nil {
		packet.AddExtra(sentry.Extra{
			"input": err.GetInput(),
		})
	}

	packet.AddError(err)
	packet.AddTag("function", cmd.Function)
	sentry.CaptureAndWait(packet)
}
