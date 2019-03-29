package saga

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/tracer"
	"github.com/rs/zerolog/log"
)

func TraceError(ctx context.Context, state *State, err errors.Error) {
	tracer.AddError(ctx, err)
}

func PrintPanic(ctx context.Context, state *State, err errors.Error) {
	if err.GetPanic() {
		fmt.Printf("%+v", err)
	}
}

func Sentry(ctx context.Context, state *State, err errors.Error) {
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