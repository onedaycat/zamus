package warmer

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/stretchr/testify/require"
)

type FakeInvoke struct {
	err errors.Error
	spy *common.SpyTest
}

func newFakeInvoke() *FakeInvoke {
	return &FakeInvoke{
		spy: common.Spy(),
	}
}

func (f *FakeInvoke) InvokeAsyncWithContext(ctx context.Context, input *lambda.InvokeAsyncInput, opts ...request.Option) (*lambda.InvokeAsyncOutput, error) {
	f.spy.Called("invoke")
	if f.err != nil {
		return nil, f.err
	}

	return nil, nil
}

func TestInvokeConcurency(t *testing.T) {
	invoker := newFakeInvoke()
	w := New(nil)
	w.ld = invoker

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	w.Run(ctx, 2)
	require.Equal(t, 2, invoker.spy.Count("invoke"))
}

func TestInvokeOne(t *testing.T) {
	invoker := newFakeInvoke()
	w := New(nil)
	w.ld = invoker

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	w.Run(ctx, 1)
	require.Equal(t, 0, invoker.spy.Count("invoke"))
}

func TestNoConcurency(t *testing.T) {
	invoker := newFakeInvoke()
	w := New(nil)
	w.ld = invoker

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	w.Run(ctx, 0)
	require.Equal(t, 0, invoker.spy.Count("invoke"))
}
