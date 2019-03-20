package warmer

import (
	"context"
	"testing"

	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/invoke"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/zamus/invoke/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestInvokeConcurency(t *testing.T) {
	invoker := &mocks.Invoker{}
	w := New(nil)
	w.ld = invoker
	spy := common.Spy()

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	req := &invoke.Request{
		Warmer:     true,
		Concurency: 0,
	}

	invoker.On("InvokeAsync", ctx, lambdacontext.FunctionName, req).Run(func(args mock.Arguments) {
		spy.Called("invoke")
	}).Return(nil)

	w.Run(ctx, 2)
	require.Equal(t, 2, spy.Count("invoke"))
	invoker.AssertExpectations(t)
}

func TestInvokeOne(t *testing.T) {
	invoker := &mocks.Invoker{}
	w := New(nil)
	w.ld = invoker
	spy := common.Spy()

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	w.Run(ctx, 1)
	require.Equal(t, 0, spy.Count("invoke"))
	invoker.AssertExpectations(t)
}

func TestNoConcurency(t *testing.T) {
	invoker := &mocks.Invoker{}
	w := New(nil)
	w.ld = invoker
	spy := common.Spy()

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	w.Run(ctx, 0)
	require.Equal(t, 0, spy.Count("invoke"))
	invoker.AssertExpectations(t)
}
