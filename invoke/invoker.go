package invoke

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type InvokeInput = lambda.InvokeInput
type InvokeOutput = lambda.InvokeOutput
type InvokeOption = request.Option
type InvokeAsyncInput = lambda.InvokeAsyncInput
type InvokeAsyncOutput = lambda.InvokeAsyncOutput

//go:generate mockery -name=Invoker
type Invoker interface {
	Invoke(input *InvokeInput) (*InvokeOutput, error)
	InvokeWithContext(ctx context.Context, input *InvokeInput, opts ...InvokeOption) (*InvokeOutput, error)
	InvokeAsync(input *InvokeAsyncInput) (*InvokeAsyncOutput, error)
	InvokeAsyncWithContext(ctx context.Context, input *InvokeAsyncInput, opts ...InvokeOption) (*InvokeAsyncOutput, error)
}
