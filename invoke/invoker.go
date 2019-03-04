package invoke

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/zamus/errors"
)

type InvokeInput = lambda.InvokeInput
type InvokeOutput = lambda.InvokeOutput
type InvokeOption = request.Option
type InvokeAsyncInput = lambda.InvokeAsyncInput
type InvokeAsyncOutput = lambda.InvokeAsyncOutput

//go:generate mockery -name=Invoker
type Invoker interface {
	Invoke(input *InvokeInput) (*InvokeOutput, errors.Error)
	InvokeWithContext(ctx context.Context, input *InvokeInput, opts ...InvokeOption) (*InvokeOutput, errors.Error)
	InvokeAsync(input *InvokeAsyncInput) (*InvokeAsyncOutput, errors.Error)
	InvokeAsyncWithContext(ctx context.Context, input *InvokeAsyncInput, opts ...InvokeOption) (*InvokeAsyncOutput, errors.Error)
}
