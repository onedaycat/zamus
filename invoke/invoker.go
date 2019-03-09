package invoke

import (
	"context"
	"encoding/json"

	"github.com/onedaycat/errors"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type InvokeInput = lambda.InvokeInput
type InvokeOutput = lambda.InvokeOutput
type InvokeOption = request.Option
type InvokeAsyncInput = lambda.InvokeAsyncInput
type InvokeAsyncOutput = lambda.InvokeAsyncOutput

var LATEST = "$LATEST"

//go:generate mockery -name=Invoker
type Invoker interface {
	Invoke(input *InvokeInput) (*InvokeOutput, error)
	InvokeWithContext(ctx context.Context, input *InvokeInput, opts ...InvokeOption) (*InvokeOutput, error)
	InvokeAsync(input *InvokeAsyncInput) (*InvokeAsyncOutput, error)
	InvokeAsyncWithContext(ctx context.Context, input *InvokeAsyncInput, opts ...InvokeOption) (*InvokeAsyncOutput, error)
}

// {"errorMessage":"Account_AccountCreated: Account already created","errorType":"AppError"}
type InvokeErrorPayload struct {
	ErrorMessage string `json:"errorMessage"`
}

func UnmarshalInvokeErrorPayload(payload []byte) errors.Error {
	in := &InvokeErrorPayload{}
	err := json.Unmarshal(payload, in)
	if err != nil {
		return errors.Wrap(err)
	}

	return errors.ParseError(in.ErrorMessage)
}
