package invoke

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
)

var LATEST = "$LATEST"

//go:generate mockery -name=Invoker
type Invoker interface {
	Invoke(ctx context.Context, fn string, req *Request, result interface{}) errors.Error
	InvokeAsync(ctx context.Context, fn string, req *Request) errors.Error
	BatchInvoke(ctx context.Context, fn string, reqs []*Request) (BatchResults, errors.Error)
	BatchInvokeAsync(ctx context.Context, fn string, reqs []*Request) errors.Error
}

//go:generate mockery -name=LambdaInvokeClient
type LambdaInvokeClient interface {
	InvokeWithContext(ctx context.Context, input *lambda.InvokeInput, opts ...request.Option) (*lambda.InvokeOutput, error)
	InvokeAsyncWithContext(ctx context.Context, input *lambda.InvokeAsyncInput, opts ...request.Option) (*lambda.InvokeAsyncOutput, error)
}

type Invoke struct {
	ld LambdaInvokeClient
}

func NewInvoke(ld LambdaInvokeClient) Invoker {
	return &Invoke{ld}
}

func (in *Invoke) Invoke(ctx context.Context, fn string, req *Request, result interface{}) errors.Error {
	reqByte, err := req.MarshalRequest()
	if err != nil {
		return err
	}

	out, xerr := in.ld.InvokeWithContext(ctx, &lambda.InvokeInput{
		FunctionName: &fn,
		Qualifier:    &LATEST,
		Payload:      reqByte,
	})
	if xerr != nil {
		return appErr.ErrUnbleInvokeFunction.WithCaller().WithInput(req).WithCause(xerr)
	}

	if out.FunctionError != nil {
		resErr, err := appErr.ParseLambdaError(out.Payload)
		if err != nil {
			return err
		}

		return resErr
	}

	if len(out.Payload) == 0 || result == nil {
		return nil
	}

	return common.UnmarshalJSON(out.Payload, result)
}

func (in *Invoke) InvokeAsync(ctx context.Context, fn string, req *Request) errors.Error {
	reqByte, err := req.MarshalRequest()
	if err != nil {
		return err
	}

	_, xerr := in.ld.InvokeAsyncWithContext(ctx, &lambda.InvokeAsyncInput{
		FunctionName: &fn,
		InvokeArgs:   bytes.NewReader(reqByte),
	})
	if xerr != nil {
		return appErr.ErrUnbleInvokeFunction.WithCaller().WithInput(req).WithCause(xerr)
	}

	return nil
}

func (in *Invoke) BatchInvoke(ctx context.Context, fn string, reqs []*Request) (BatchResults, errors.Error) {
	if len(reqs) > 10 {
		return nil, appErr.ErrBatchRequestExceed
	}

	reqsByte, err := common.MarshalJSON(reqs)
	if err != nil {
		return nil, err
	}

	out, xerr := in.ld.InvokeWithContext(ctx, &lambda.InvokeInput{
		FunctionName: &fn,
		Qualifier:    &LATEST,
		Payload:      reqsByte,
	})
	if xerr != nil {
		return nil, appErr.ErrUnbleInvokeFunction.WithCaller().WithInput(reqs).WithCause(xerr)
	}

	if out.FunctionError != nil {
		resErr, err := appErr.ParseLambdaError(out.Payload)
		if err != nil {
			return nil, err
		}

		return nil, resErr
	}

	if len(out.Payload) == 0 {
		return BatchResults{}, nil
	}

	results := BatchResults{}
	if err := common.UnmarshalJSON(out.Payload, &results); err != nil {
		return nil, err
	}

	for i := 0; i < len(results); i++ {
		if results[i].Error != nil {
			results[i].Error = appErr.ErrorByCode(results[i].Error).(*errors.AppError)
		}
	}

	return results, nil
}

func (in *Invoke) BatchInvokeAsync(ctx context.Context, fn string, reqs []*Request) errors.Error {
	if len(reqs) > 10 {
		return appErr.ErrBatchRequestExceed
	}

	reqsByte, err := common.MarshalJSON(reqs)
	if err != nil {
		return err
	}

	_, xerr := in.ld.InvokeAsyncWithContext(ctx, &lambda.InvokeAsyncInput{
		FunctionName: &fn,
		InvokeArgs:   bytes.NewReader(reqsByte),
	})
	if xerr != nil {
		return appErr.ErrUnbleInvokeFunction.WithCaller().WithInput(reqs).WithCause(xerr)
	}

	return nil
}
