package trigger

import (
	"context"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/zamuscontext"
)

var zcctx *zamuscontext.ZamusContext

type Config struct {
	AppStage      string
	Service       string
	Version       string
	SentryRelease string
	SentryDNS     string
	EnableTrace   bool
}

func init() {
	common.PrettyLog()
	xray.SetLogger(xraylog.NullLogger)
}

func InitTrigger(config *Config) {
	if config.SentryDNS != "" {
		sentry.SetDSN(config.SentryDNS)
		sentry.SetOptions(
			sentry.WithEnv(config.AppStage),
			sentry.WithRelease(config.SentryRelease),
			sentry.WithServerName(lambdacontext.FunctionName),
			sentry.WithServiceName(config.Service),
			sentry.WithVersion(config.Version),
			sentry.WithTags(sentry.Tags{
				{"lambdaVersion", lambdacontext.FunctionVersion},
			}),
		)
	}

	tracer.Enable = config.EnableTrace

	zcctx = &zamuscontext.ZamusContext{
		AppStage:       config.AppStage,
		Service:        config.Service,
		LambdaFunction: lambdacontext.FunctionName,
		LambdaVersion:  lambdacontext.FunctionVersion,
		Version:        config.Version,
	}
}

func InitContext(ctx context.Context) context.Context {
	return zamuscontext.NewContext(ctx, zcctx)
}
