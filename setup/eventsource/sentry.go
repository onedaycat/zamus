package main

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, event *events.KinesisFirehoseEvent, err errors.Error) {
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
