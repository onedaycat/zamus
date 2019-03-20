package service

import (
	"context"
	"fmt"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/tracer"
	"github.com/rs/zerolog/log"
)

func PrintPanic(ctx context.Context, req *Request, err errors.Error) {
	if err.GetPanic() {
		fmt.Printf("%+v", err)
	}
}

func TraceError(ctx context.Context, req *Request, err errors.Error) {
	tracer.AddError(ctx, err)
}

func Sentry(ctx context.Context, req *Request, err errors.Error) {
	switch err.GetStatus() {
	case errors.InternalErrorStatus:
		log.Error().
			Interface("input", err.GetInput()).
			Msg(err.Error())
	default:
		return
	}

	packet := sentry.NewPacket(err)
	if req.Identity != nil && req.Identity.ID != "" {
		packet.AddUser(&sentry.User{
			ID: req.Identity.ID,
		})
	}

	packet.AddError(err)
	packet.AddTag("function", req.Function)
	sentry.CaptureAndWait(packet)
}
