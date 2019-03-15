package query

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func PrintPanic(ctx context.Context, req *QueryReq, err errors.Error) {
	if err.GetPanic() {
		fmt.Printf("%+v", err)
	}
}

func Sentry(ctx context.Context, req *QueryReq, appErr errors.Error) {
	switch appErr.GetStatus() {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", appErr.GetInput()).
			Msg(appErr.Error())
	default:
		return
	}

	packet := sentry.NewPacket(appErr)
	if req.Identity != nil && req.Identity.GetID() != "" {
		packet.AddUser(&sentry.User{
			ID: req.Identity.GetID(),
		})
	}

	packet.AddError(appErr)
	packet.AddTag("function", req.Function)
	sentry.CaptureAndWait(packet)
}
