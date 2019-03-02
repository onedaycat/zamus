package query

import (
	"context"

	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/rs/zerolog/log"
)

func Sentry(dsn string, options ...sentry.Option) ErrorHandler {
	sentry.SetDSN(dsn)
	sentry.SetOptions(options...)

	return func(ctx context.Context, query *Query, err error) {
		var appErr *errors.AppError
		var ok bool

		if appErr, ok = errors.FromError(err); ok {
			switch errors.ErrStatus(err) {
			case errors.InternalErrorStatus:
				log.Error().
					Interface("input", appErr.Input).
					Msg(appErr.Error())
			default:
				return
			}
		} else {
			log.Error().Msg(err.Error())
			appErr = errors.WithCaller(err)
		}

		packet := sentry.NewPacket(err)
		if query.Identity != nil && query.Identity.GetID() != "" {
			packet.AddUser(&sentry.User{
				ID: query.Identity.GetID(),
			})
		}

		if appErr.Input != nil {
			packet.AddExtra(sentry.Extra{
				"input": appErr.Input,
			})
		}

		if seg := xray.GetSegment(ctx); seg != nil {
			seg.AddAnnotation("error", true)
			seg.AddAnnotation("error_code", appErr.Code)
			seg.AddAnnotation("error_msg", appErr.Error())
			seg.Error = true
		}

		packet.SetFingerprint(appErr.Code)
		packet.SetCulprit(appErr.Message)
		packet.AddTag("function", query.Function)
		packet.AddStackTrace(appErr.StackTrace())
		sentry.CaptureAndWait(packet)
	}
}
