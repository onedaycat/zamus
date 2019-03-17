package eventhandler

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/tracer"
	"github.com/rs/zerolog/log"
)

func TraceError(ctx context.Context, msgs EventMsgs, err errors.Error) {
	tracer.AddError(ctx, err)
}

func PrintPanic(ctx context.Context, msgs EventMsgs, err errors.Error) {
	if err.GetPanic() {
		fmt.Printf("%+v", err)
	}
}

func Sentry(ctx context.Context, msgs EventMsgs, err errors.Error) {
	switch err.GetStatus() {
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
