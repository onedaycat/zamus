package command

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestCommandHandler(t *testing.T) {
	checkFunc := false

	cmd := &Command{
		Function:      "testHandlerCommand",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "deleteWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, event *Command) (interface{}, error) {
		require.False(t, checkFunc)
		checkFunc = true
		return nil, nil
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommand", f, WithPermission("deleteWorkspace"))

	resp, err := h.handler(context.Background(), cmd)

	require.Nil(t, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
}

func TestCommandPermissionDenied(t *testing.T) {
	checkFunc := false

	cmd := &Command{
		Function:      "testHandlerCommandDenied",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "editWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, event *Command) (interface{}, error) {
		require.False(t, checkFunc)
		checkFunc = true

		return nil, nil
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommandDenied", f, WithPermission("deleteWorkspace"))

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Equal(t, ErrPermissionDenied, err)
	require.Nil(t, resp)
	require.False(t, checkFunc)
}

func TestCommandPreHandler(t *testing.T) {
	checkFunc := false
	preFunc := false
	preFunc2 := false

	cmd := &Command{
		Function:      "testHandlerCommand",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "deleteWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, event *Command) (interface{}, error) {
		require.False(t, checkFunc)
		checkFunc = true
		return nil, nil
	}

	fPre := func(ctx context.Context, event *Command) (interface{}, error) {
		require.False(t, preFunc)
		preFunc = true
		return nil, nil
	}

	fPre2 := func(ctx context.Context, event *Command) (interface{}, error) {
		require.False(t, preFunc2)
		preFunc2 = true
		return nil, nil
	}

	h := NewHandler()
	h.PreHandler(fPre, fPre2)
	h.RegisterCommand("testHandlerCommand", f, WithPermission("deleteWorkspace"))

	resp, err := h.handler(context.Background(), cmd)

	require.Nil(t, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, preFunc)
	require.True(t, preFunc2)
}

func TestErrorHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	cmd := &Command{
		Function:      "testHandlerCommandError",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "deleteWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, event *Command) (interface{}, error) {
		require.False(t, checkFunc)
		checkFunc = true
		return nil, errors.BadRequest("60000", "Invalidation workspaceID")
	}

	fError := func(ctx context.Context, event *Command, err error) {
		errorFunc = true
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommandError", f, WithPermission("deleteWorkspace"))
	h.ErrorHandler(fError)

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Equal(t, "60000: Invalidation workspaceID", err.Error())
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}
