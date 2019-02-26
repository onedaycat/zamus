package command

import (
	"context"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(dsn string, options ...sentry.Option) ErrorHandler {
	sentry.SetDSN(dsn)
	sentry.SetOptions(options...)

	return func(ctx context.Context, cmd *Command, err error) {
		var appErr *errors.AppError

		appErr, _ = errors.FromError(err)
		switch errors.ErrStatus(err) {
		case errors.InternalErrorStatus:
			log.Error().
				Interface("input", appErr.Input).
				Msg(appErr.Error())
		default:
			return
		}

		packet := sentry.NewPacket(err)
		if cmd.Identity != nil && cmd.Identity.GetID() != "" {
			packet.AddUser(&sentry.User{
				ID: cmd.Identity.GetID(),
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

		packet.AddTag("lambda", lambdacontext.FunctionName)
		packet.AddTag("function", cmd.Function)

		packet.AddStackTrace(appErr.StackTrace())
		sentry.CaptureAndWait(packet)
	}
}
