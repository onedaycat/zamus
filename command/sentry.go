package command

import (
	"context"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/invoke"
	"github.com/rs/zerolog/log"
)

func SentryError(dsn string, options ...sentry.Option) ErrorHandler {
	sentry.SetDSN(dsn)
	sentry.SetOptions(options...)

	return func(ctx context.Context, event *invoke.InvokeEvent, err error) {
		var appErr *errors.AppError

		appErr, _ = errors.FromError(err)
		switch errors.ErrStatus(err) {
		case errors.InternalErrorStatus:
			log.Error().
				Interface("input", appErr.Input).
				Msgf("%+v\n", appErr)
		default:
			return
		}

		packet := sentry.NewPacket(err)
		if event.Identity.GetID() != "" {
			packet.AddUser(&sentry.User{
				ID: event.Identity.GetID(),
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

		funcName := ctx.Value(lambdacontext.FunctionName).(string)
		packet.AddTag("lambda", funcName)
		packet.AddTag("function", event.Function)

		packet.AddStackTrace(appErr.StackTrace())
		sentry.CaptureAndWait(packet)
	}
}
