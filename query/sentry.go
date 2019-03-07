package query

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, query *Query, appErr errors.Error) {
	switch appErr.GetStatus() {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", appErr.GetInput()).
			Msg(appErr.Error())
	default:
		return
	}

	packet := sentry.NewPacket(appErr)
	if query.Identity != nil && query.Identity.GetID() != "" {
		packet.AddUser(&sentry.User{
			ID: query.Identity.GetID(),
		})
	}

	packet.AddError(appErr)
	packet.AddTag("function", query.Function)
	sentry.CaptureAndWait(packet)
}
