package errors

import (
	"testing"

	"github.com/onedaycat/errors"

	"github.com/stretchr/testify/require"
)

func TestParseLambdaError(t *testing.T) {

	payload := `{"errorType":"LambdaError", "errorMessage": "{\"type\":\"InternalError\",\"status\":500,\"code\":\"InternalError\",\"message\":\"hello\"}"}`
	appErr, err := ParseLambdaError([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, ErrInternalError, appErr)

	payload = `{"errorType":"LambdaError", "errorMessage": "{\"type\":\"BadRequest\",\"status\":400,\"code\":\"ValidateError\",\"message\":\"hello\"}"}`
	appErr, err = ParseLambdaError([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, ErrValidateError.WithMessage("hello"), appErr)

	payload = `{"errorType":"LambdaError", "errorMessage": "{\"type\":\"BadRequest\",\"status\":400,\"code\":\"Hello\",\"message\":\"hello\"}"}`
	appErr, err = ParseLambdaError([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, errors.BadRequest("Hello", "hello"), appErr)

	payload = `{"errorType":"AppError", "errorMessage": "hello"}`
	appErr, err = ParseLambdaError([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, errors.InternalError("AppError", "hello"), appErr)
}
