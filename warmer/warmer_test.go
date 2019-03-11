package warmer

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/invoke/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestInvokeConcurency(t *testing.T) {
	invoker := &mocks.Invoker{}
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	spy := common.Spy()
	w := New(sess)
	w.ld = invoker

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	invoker.On("Invoke", mock.Anything).Run(func(args mock.Arguments) {
		spy.Called("invoke")
	}).Return(nil, nil)

	w.Run(ctx, 3)
	require.Equal(t, 3, spy.Count("invoke"))
	invoker.AssertExpectations(t)
}

func TestInvokeOne(t *testing.T) {
	invoker := &mocks.Invoker{}
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	spy := common.Spy()
	w := New(sess)
	w.ld = invoker

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	invoker.On("Invoke", mock.Anything).Run(func(args mock.Arguments) {
		spy.Called("invoke")
	}).Return(nil, nil)

	w.Run(ctx, 1)
	require.Equal(t, 1, spy.Count("invoke"))
	invoker.AssertExpectations(t)
}

func TestNoConcurency(t *testing.T) {
	invoker := &mocks.Invoker{}
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	spy := common.Spy()
	w := New(sess)
	w.ld = invoker

	ctx := context.Background()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{
		AwsRequestID: "req1",
	})

	w.Run(ctx, 0)
	require.Equal(t, 0, spy.Count("invoke"))
}
