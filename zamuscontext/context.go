package zamuscontext

import "context"

type zamusKey struct{}

var zamuscontextKey = &zamusKey{}

type ZamusContext struct {
	AppStage       string
	Service        string
	LambdaFunction string
	LambdaVersion  string
	Version        string
}

func NewContext(parent context.Context, zc *ZamusContext) context.Context {
	return context.WithValue(parent, zamuscontextKey, zc)
}

func FromContext(ctx context.Context) (*ZamusContext, bool) {
	zc, ok := ctx.Value(zamuscontextKey).(*ZamusContext)
	return zc, ok
}
