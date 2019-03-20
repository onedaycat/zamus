package invoke_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/invoke/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestInvoke(t *testing.T) {
	ctx := context.Background()
	args := map[string]interface{}{
		"id": "1",
	}
	mockResult := map[string]interface{}{
		"name": "name1",
	}
	mockResultByte, _ := common.MarshalJSON(mockResult)
	fn := "fn1"
	errUnhandled := "Unhandled"

	t.Run("Success", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		result := map[string]interface{}{}
		req := invoke.NewRequest("m1").WithArgs(args).WithIdentity(&invoke.Identity{}).WithPermission("hello", "read")
		reqByte, _ := req.MarshalRequest()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       mockResultByte,
			FunctionError: nil,
		}, nil)

		err := in.Invoke(ctx, fn, req, &result)

		require.NoError(t, err)
		require.Equal(t, result, mockResult)
		ld.AssertExpectations(t)
	})

	t.Run("Success But No Data", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		result := map[string]interface{}{}
		// builder := random.InvokeReq("m1").Args(args)
		req := invoke.NewRequest("m1").WithArgs(args).WithPermission("hello", "read")
		reqByte, _ := req.MarshalRequest()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       nil,
			FunctionError: nil,
		}, nil)

		err := in.Invoke(ctx, fn, req, &result)

		require.NoError(t, err)
		require.Len(t, result, 0)
		ld.AssertExpectations(t)
	})

	t.Run("Success But dont need result", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReq("m1").Args(args)
		req := builder.Build()
		reqByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       mockResultByte,
			FunctionError: nil,
		}, nil)

		err := in.Invoke(ctx, fn, req, nil)

		require.NoError(t, err)
		ld.AssertExpectations(t)
	})

	t.Run("Error From payload", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		result := map[string]interface{}{}
		builder := random.InvokeReq("m1").Args(args)
		req := builder.Build()
		reqByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       []byte(`{"errorType":"LambdaError", "errorMessage": "{\"type\":\"BadRequest\",\"status\":400,\"code\":\"Hello\",\"message\":\"hello\"}"}`),
			FunctionError: &errUnhandled,
		}, nil)

		err := in.Invoke(ctx, fn, req, &result)

		require.Equal(t, errors.BadRequest("Hello", "hello"), err)
		require.Len(t, result, 0)
		ld.AssertExpectations(t)
	})

	t.Run("Invoke Error", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		result := map[string]interface{}{}
		builder := random.InvokeReq("m1").Args(args)
		req := builder.Build()
		reqByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqByte,
		}).Return(nil, errors.DumbError)

		err := in.Invoke(ctx, fn, req, &result)

		require.Equal(t, appErr.ErrUnbleInvokeFunction, err)
		require.Len(t, result, 0)
		ld.AssertExpectations(t)
	})
}

func TestInvokeAsync(t *testing.T) {
	ctx := context.Background()
	args := map[string]interface{}{
		"id": "1",
	}

	fn := "fn1"

	t.Run("Success", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReq("m1").Args(args)
		req := builder.Build()
		reqByte := builder.BuildJSON()

		ld.On("InvokeAsyncWithContext", mock.Anything, &lambda.InvokeAsyncInput{
			FunctionName: &fn,
			InvokeArgs:   bytes.NewReader(reqByte),
		}).Return(nil, nil)

		err := in.InvokeAsync(ctx, fn, req)

		require.NoError(t, err)
		ld.AssertExpectations(t)
	})

	t.Run("Invoke Error", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReq("m1").Args(args)
		req := builder.Build()
		reqByte := builder.BuildJSON()

		ld.On("InvokeAsyncWithContext", mock.Anything, &lambda.InvokeAsyncInput{
			FunctionName: &fn,
			InvokeArgs:   bytes.NewReader(reqByte),
		}).Return(nil, errors.DumbError)

		err := in.InvokeAsync(ctx, fn, req)

		require.Equal(t, appErr.ErrUnbleInvokeFunction, err)
		ld.AssertExpectations(t)
	})

}

func TestBatchInvoke(t *testing.T) {
	ctx := context.Background()
	args := []map[string]interface{}{
		{"id": "1"},
		{"id": "2"},
		{"id": "3"},
	}
	mockResults := invoke.BatchResults{
		{Data: map[string]interface{}{"id": "1"}},
		{Data: map[string]interface{}{"id": "2"}},
		{Data: map[string]interface{}{"id": "3"}},
	}

	mockResultsWithError := invoke.BatchResults{
		{Data: map[string]interface{}{"id": "1"}},
		{Error: appErr.ErrNotImplement},
		{Data: map[string]interface{}{"id": "3"}},
	}

	mockResultsByte, _ := common.MarshalJSON(mockResults)
	mockResultsWithErrorByte, _ := common.MarshalJSON(mockResultsWithError)

	fn := "fn1"

	t.Run("Success", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReqs().Add("m1", args)
		reqs := builder.Build()
		reqsByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqsByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       mockResultsByte,
			FunctionError: nil,
		}, nil)

		results, err := in.BatchInvoke(ctx, fn, reqs)

		require.NoError(t, err)
		require.Equal(t, mockResults, results)
		ld.AssertExpectations(t)
	})

	t.Run("Success But No Data", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReqs().Add("m1", args)
		reqs := builder.Build()
		reqsByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqsByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       nil,
			FunctionError: nil,
		}, nil)

		results, err := in.BatchInvoke(ctx, fn, reqs)

		require.NoError(t, err)
		require.Len(t, results, 0)
		ld.AssertExpectations(t)
	})

	t.Run("Error From payload", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReqs().Add("m1", args)
		reqs := builder.Build()
		reqsByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqsByte,
		}).Return(&lambda.InvokeOutput{
			Payload:       mockResultsWithErrorByte,
			FunctionError: nil,
		}, nil)

		results, err := in.BatchInvoke(ctx, fn, reqs)

		require.NoError(t, err)
		require.Equal(t, mockResultsWithError, results)
		ld.AssertExpectations(t)
	})

	t.Run("Invoke Error", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReqs().Add("m1", args)
		reqs := builder.Build()
		reqsByte := builder.BuildJSON()

		ld.On("InvokeWithContext", mock.Anything, &lambda.InvokeInput{
			FunctionName: &fn,
			Qualifier:    &invoke.LATEST,
			Payload:      reqsByte,
		}).Return(nil, errors.DumbError)

		results, err := in.BatchInvoke(ctx, fn, reqs)

		require.Equal(t, appErr.ErrUnbleInvokeFunction, err)
		require.Len(t, results, 0)
		ld.AssertExpectations(t)
	})
}

func TestBatchInvokeAsync(t *testing.T) {
	ctx := context.Background()
	args := []map[string]interface{}{
		{"id": "1"},
		{"id": "2"},
		{"id": "3"},
	}

	fn := "fn1"

	t.Run("Success", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReqs().Add("m1", args)
		reqs := builder.Build()
		reqsByte := builder.BuildJSON()

		ld.On("InvokeAsyncWithContext", mock.Anything, &lambda.InvokeAsyncInput{
			FunctionName: &fn,
			InvokeArgs:   bytes.NewReader(reqsByte),
		}).Return(nil, nil)

		err := in.BatchInvokeAsync(ctx, fn, reqs)

		require.NoError(t, err)
		ld.AssertExpectations(t)
	})

	t.Run("Invoke Error", func(t *testing.T) {
		ld := &mocks.LambdaInvokeClient{}
		in := invoke.NewInvoke(ld)

		builder := random.InvokeReqs().Add("m1", args)
		reqs := builder.Build()
		reqsByte := builder.BuildJSON()

		ld.On("InvokeAsyncWithContext", mock.Anything, &lambda.InvokeAsyncInput{
			FunctionName: &fn,
			InvokeArgs:   bytes.NewReader(reqsByte),
		}).Return(nil, errors.DumbError)

		err := in.BatchInvokeAsync(ctx, fn, reqs)

		require.Equal(t, appErr.ErrUnbleInvokeFunction, err)
		ld.AssertExpectations(t)
	})
}
