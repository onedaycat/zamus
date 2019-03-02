package zamuscontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContext(t *testing.T) {

	zc := &ZamusContext{
		Service:        "srv1",
		LambdaFunction: "lambda1",
		LambdaVersion:  "$LATEST",
		Version:        "1.0.0",
		SentryRelease:  "project@v1.2.4",
	}

	ctx := context.Background()
	ctx = NewContext(ctx, zc)

	zc2, ok := FromContext(ctx)
	require.True(t, ok)
	require.Equal(t, zc, zc2)
}

func TestContextNotFound(t *testing.T) {
	zc, ok := FromContext(context.Background())
	require.False(t, ok)
	require.Nil(t, zc)
}
