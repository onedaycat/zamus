package command

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/stretchr/testify/require"
)

func TestSentry(t *testing.T) {
	f := func(ctx context.Context, req *CommandReq) (interface{}, errors.Error) {
		return nil, errors.New("111")
	}

	h := NewHandler(&Config{})
	h.RegisterCommand("hello", f)
	h.ErrorHandlers(Sentry)

	req := NewCommandReq("hello")

	resp, err := h.Handle(context.Background(), req)

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestSentryWithAppError(t *testing.T) {
	f := func(ctx context.Context, req *CommandReq) (interface{}, errors.Error) {
		return nil, errors.InternalError("IN1", "IN_ERR").WithCaller().WithInput(errors.Input{"in": "put"}).WithCause(errors.New("cause"))
	}

	h := NewHandler(&Config{})
	h.RegisterCommand("hello", f)
	h.ErrorHandlers(Sentry)

	cmd := NewCommandReq("hello")
	cmd.Identity = &Identity{
		Username: "u1",
	}

	resp, err := h.Handle(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
}
