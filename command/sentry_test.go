package command

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestSentry(t *testing.T) {
	f := func(ctx context.Context, cmd *Command) (interface{}, errors.Error) {
		return nil, errors.New("111")
	}

	h := NewHandler(&Config{})
	h.RegisterCommand("hello", f)
	h.ErrorHandlers(Sentry)

	cmd := &Command{
		Function: "hello",
	}

	resp, err := h.Handle(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestSentryWithAppError(t *testing.T) {
	f := func(ctx context.Context, cmd *Command) (interface{}, errors.Error) {
		return nil, errors.InternalError("IN1", "IN_ERR").WithCaller().WithInput(errors.Input{"in": "put"}).WithCause(errors.New("cause"))
	}

	h := NewHandler(&Config{})
	h.RegisterCommand("hello", f)
	h.ErrorHandlers(Sentry)

	cmd := &Command{
		Function: "hello",
		Identity: &invoke.Identity{
			Username: "u1",
		},
	}

	resp, err := h.Handle(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
}
