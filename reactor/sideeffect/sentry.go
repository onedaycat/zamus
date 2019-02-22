package sideeffect

import (
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(dsn string, options ...sentry.Option) ErrorHandler {
	sentry.SetDSN(dsn)
	sentry.SetOptions(options...)

	return func(msgs EventMsgs, err error) {
		var appErr *errors.AppError
		var ok bool

		if appErr, ok = errors.FromError(err); ok {
			switch errors.ErrStatus(err) {
			case errors.BadRequestStatus:
				return
			default:
				log.Error().
					Interface("input", appErr.Input).
					Msgf("%+v\n", appErr)
			}
		} else {
			log.Error().Msg(err.Error())
			appErr = errors.WithCaller(err)
		}

		packet := sentry.NewPacket(err)

		if appErr.Input != nil {
			packet.AddExtra(sentry.Extra{
				"input": appErr.Input,
			})
		}

		if appErr.Cause != nil {
			packet.AddExtra(sentry.Extra{
				"cause": appErr.Cause.Error(),
			})
		}

		packet.AddStackTrace(appErr.StackTrace())
		sentry.CaptureAndWait(packet)
	}
}
