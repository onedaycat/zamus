package dispatcher

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/sentry"
    "github.com/onedaycat/zamus/reactor/source/dynamostream"
    "github.com/rs/zerolog/log"
)

func Sentry(ctx context.Context, stream *dynamostream.Source, err errors.Error) {
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
