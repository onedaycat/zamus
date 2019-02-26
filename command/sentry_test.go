package command

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestSentry(t *testing.T) {
	errHandler := Sentry("test")

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		return nil, errors.New("111")
	}

	h := NewHandler()
	h.RegisterCommand("hello", f)
	h.ErrorHandlers(errHandler)

	cmd := &Command{
		Function: "hello",
	}

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestSentryWithAppError(t *testing.T) {
	errHandler := Sentry("test")

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		return nil, errors.InternalError("IN1", "IN_ERR").WithCaller().WithInput(errors.Input{"in": "put"}).WithCause(errors.New("cause"))
	}

	h := NewHandler()
	h.RegisterCommand("hello", f)
	h.ErrorHandlers(errHandler)

	cmd := &Command{
		Function: "hello",
		Identity: &invoke.Identity{
			Username: "u1",
		},
	}

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
}
