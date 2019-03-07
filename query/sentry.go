package query

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func PrintPanic(ctx context.Context, query *Query, err errors.Error) {
	if err.GetPanic() {
		if cause := err.GetCause(); cause != nil {
			fmt.Println(cause.Error())
		} else if input := err.GetInput(); input != nil {
			fmt.Println(input)
		}
	}
}

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
