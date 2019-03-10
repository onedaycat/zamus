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

// ErrInternalError    = errors.InternalError("InternalError", "Internal error")
// 	ErrInvalidRequest   = errors.BadRequest("InvalidRequest", "Invalid request")
// 	ErrValidateError    = errors.BadRequest("ValidateError", "Validation error")
// 	ErrPermissionDenied = errors.Forbidden("PermissionDenied", "You don't a permission to access this operation")
// 	ErrTimeout          = errors.Timeout("TimeoutError", "The operation is timeout")
// 	ErrUnauthorized     = errors.Unauthorized("Unauthorized", "The authorization is required")
// 	ErrUnavailable      = errors.Unavailable("Unavailable", "This operation is unavailable")

func UnmarshalInvokeErrorPayload(payload []byte) errors.Error {
	in := &InvokeErrorPayload{}
	err := json.Unmarshal(payload, in)
	if err != nil {
		return errors.Wrap(err)
	}

	inappErr := errors.ParseError(in.ErrorMessage)
	switch inappErr.Code {
	case "InternalError":
		return inappErr.WithInternalError()
	case "InvalidRequest":
		return inappErr.WithBadRequest()
	case "ValidateError":
		return inappErr.WithBadRequest()
	case "PermissionDenied":
		return inappErr.WithForbidden()
	case "TimeoutError":
		return inappErr.WithTimeoutError()
	case "Unauthorized":
		return inappErr.WithUnauthorized()
	case "Unavailable":
		return inappErr.WithBadRequest()
	}

	return inappErr.WithType(inappErr.Code)
}
