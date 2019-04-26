package publisher

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/tracer"
)

//noinspection GoUnusedParameter
func TraceError(ctx context.Context, err errors.Error) {
	tracer.AddError(ctx, err)
}
