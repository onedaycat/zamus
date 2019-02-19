package sagas

import (
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
	"github.com/rs/zerolog/log"
)

func Sentry(dsn string, options ...sentry.Option) ErrorHandler {
	sentry.SetDSN(dsn)
	sentry.SetOptions(options...)

	return func(msg *kinesisstream.EventMsg, err error) {
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
		if msg.UserID != "" {
			packet.AddUser(&sentry.User{
				ID: msg.UserID,
			})
		}

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
