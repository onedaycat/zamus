package trigger

import (
	"context"

	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/common"
	"github.com/rs/zerolog/log"
)

func Init() {
	common.PrettyLog()
	xray.SetLogger(xraylog.NullLogger)
}

func Sentry(ctx context.Context, id string, err error) {
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
	if id != "" {
		packet.AddExtra(sentry.Extra{
			"id": id,
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
	packet.AddStackTrace(appErr.StackTrace())
	sentry.CaptureAndWait(packet)
}
