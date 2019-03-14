package command

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func PrintPanic(ctx context.Context, cmd *CommandReq, err errors.Error) {
	if err.GetPanic() {
		if cause := err.GetCause(); cause != nil {
			fmt.Println(cause.Error())
		} else if input := err.GetInput(); input != nil {
			fmt.Println(input)
		}
	}
}

func Sentry(ctx context.Context, req *CommandReq, err errors.Error) {
	switch errors.ErrStatus(err) {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", err.GetInput()).
			Msg(err.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)
	if req.Identity != nil && req.Identity.GetID() != "" {
		packet.AddUser(&sentry.User{
			ID: req.Identity.GetID(),
		})
	}

	packet.AddError(err)
	packet.AddTag("function", req.Function)
	sentry.CaptureAndWait(packet)
}
