package reactor

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"

	"github.com/onedaycat/zamus/tracer"
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
	if !err.HasStackTrace() {
		return
	}

	l := log.Error()
	if input := err.GetInput(); input != nil {
		l.Interface("input", input)
	}
	if cause := err.GetCause(); cause != nil {
		l.Interface("cause", cause.Error())
	}
	l.Msg(err.Error())

	packet := sentry.NewPacket(err)

	packet.AddError(err)
	sentry.CaptureAndWait(packet)
}
