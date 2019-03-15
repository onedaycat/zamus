package sideeffect

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func PrintPanic(ctx context.Context, msgs EventMsgs, err errors.Error) {
	if err.GetPanic() {
		fmt.Printf("%+v", err)
	}
}

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

	packet.AddError(err)
	sentry.CaptureAndWait(packet)
}
