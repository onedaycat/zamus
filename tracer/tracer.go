package tracer

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
)

type Segment = xray.Segment

var (
	Enable = false
)

func init() {
	xray.SetLogger(xraylog.NullLogger)
	common.PrettyLog()
}

func AWS(c *client.Client) {
	if Enable {
		xray.AWS(c)
	}
}

func BeginSubsegment(ctx context.Context, name string) (context.Context, *Segment) {
	if Enable {
		return xray.BeginSubsegment(ctx, name)
	}

	return xray.BeginFacadeSegment(ctx, name, nil)
}

func GetSegment(ctx context.Context) *Segment {
	return xray.GetSegment(ctx)
}

func Close(seg *Segment) {
	if seg == nil {
		return
	}

	seg.Close(nil)
}

func AddError(seg *Segment, appErr errors.Error) {
	if !Enable || seg == nil {
		return
	}

	seg.AddAnnotation("error", appErr.GetStatus())
	seg.AddAnnotation("error_code", appErr.GetCode())
	seg.AddAnnotation("error_msg", appErr.Error())
	seg.AddAnnotation("panic", appErr.GetPanic())

	switch appErr.GetStatus() {
	case errors.InternalErrorStatus:
		seg.Fault = true
	default:
		seg.Error = true
	}
}
