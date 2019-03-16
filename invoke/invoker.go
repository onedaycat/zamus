package invoke

import (
	"context"

	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/errors"
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
	err := common.UnmarshalJSON(payload, in)
	if err != nil {
		return err
	}

	inappErr := errors.ParseError(in.ErrorMessage)
	switch inappErr.Code {
	case appErr.ErrInternalErrorType:
		return inappErr.WithInternalError()
	case appErr.ErrInvalidRequestType:
		return inappErr.WithBadRequest()
	case appErr.ErrValidateErrorType:
		return inappErr.WithBadRequest()
	case appErr.ErrPermissionDeniedType:
		return inappErr.WithForbidden()
	case appErr.ErrTimeoutType:
		return inappErr.WithTimeoutError()
	case appErr.ErrUnauthorizedType:
		return inappErr.WithUnauthorized()
	case appErr.ErrUnavailableType:
		return inappErr.WithBadRequest()
	case appErr.ErrNotImplementType:
		return inappErr.WithType(errors.NotImplementType).WithStatus(errors.NotImplementStatus)
	case appErr.ErrUnableUnmarshalType:
		return inappErr.WithInternalError()
	case appErr.ErrUnableMarshalType:
		return inappErr.WithInternalError()
	case appErr.ErrUnableEncodeType:
		return inappErr.WithInternalError()
	case appErr.ErrUnableDecodeType:
		return inappErr.WithInternalError()
	}

	return inappErr.WithType(inappErr.Code)
}
