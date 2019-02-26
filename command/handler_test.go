package command

import (
	"context"
	"fmt"
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

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
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

func TestCommandNotFound(t *testing.T) {
	checkFunc := false

	cmd := &Command{
		Function: "xxxxxx",
	}

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		checkFunc = true
		return nil, nil
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, ErrCommandNotFound("xxxxxx"), err)
	require.Nil(t, resp)
	require.False(t, checkFunc)
}

func TestCommandPreHandler(t *testing.T) {
	nH := 0
	nFPre := 0
	nFErr := 0

	cmd := &Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nH++
		return nil, nil
	}

	fPre := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPre++
		return nil, nil
	}

	fErr := func(ctx context.Context, cmd *Command, err error) {
		nFErr++
	}

	h := NewHandler()

	h.PreHandlers(fPre, fPre)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.handler(context.Background(), cmd)

	require.Nil(t, err)
	require.Equal(t, nil, resp)
	require.Equal(t, 1, nH)
	require.Equal(t, 2, nFPre)
	require.Equal(t, 0, nFErr)
}

func TestCommandPreHandlerError(t *testing.T) {
	nH := 0
	nFPre := 0
	nFPreErr := 0
	nFErr := 0

	cmd := &Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nH++
		return nil, nil
	}

	fPre := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPre++
		return nil, nil
	}

	fPreErr := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPreErr++
		return nil, errors.New("err")
	}

	fErr := func(ctx context.Context, cmd *Command, err error) {
		nFErr++
	}

	h := NewHandler()

	h.PreHandlers(fPreErr, fPre)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Equal(t, nil, resp)
	require.Equal(t, 0, nH)
	require.Equal(t, 0, nFPre)
	require.Equal(t, 1, nFPreErr)
	require.Equal(t, 1, nFErr)
}

func TestCommandPreHandlerResult(t *testing.T) {
	nH := 0
	nFPre := 0
	nFErr := 0

	cmd := &Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nH++
		return nil, nil
	}

	fPre := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPre++
		return nil, nil
	}

	fPre2 := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPre++
		return 1, nil
	}

	fErr := func(ctx context.Context, cmd *Command, err error) {
		nFErr++
	}

	h := NewHandler()

	h.PreHandlers(fPre, fPre2)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.handler(context.Background(), cmd)

	require.Nil(t, err)
	require.Equal(t, 1, resp)
	require.Equal(t, 0, nH)
	require.Equal(t, 2, nFPre)
	require.Equal(t, 0, nFErr)
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

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		require.False(t, checkFunc)
		checkFunc = true
		return nil, errors.BadRequest("60000", "Invalidation workspaceID")
	}

	fError := func(ctx context.Context, cmd *Command, err error) {
		errorFunc = true
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommandError", f, WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fError)

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Equal(t, "60000: Invalidation workspaceID", err.Error())
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestPanicHandler(t *testing.T) {
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

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		require.False(t, checkFunc)
		checkFunc = true
		var e *Command
		_ = e.Args

		return nil, nil
	}

	fError := func(ctx context.Context, cmd *Command, err error) {
		errorFunc = true
		fmt.Println(err)
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommandError", f, WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fError)

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestCommandPostHandler(t *testing.T) {
	nH := 0
	nFPost1 := 0
	nFPost2 := 0

	cmd := &Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nH++
		return nil, nil
	}

	fPost1 := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPost1++
		return 1, nil
	}

	fPost2 := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPost2++
		return nil, nil
	}

	h := NewHandler()
	h.PostHandlers(fPost1, fPost2)
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.handler(context.Background(), cmd)

	require.Nil(t, err)
	require.Equal(t, 1, resp)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nFPost1)
	require.Equal(t, 0, nFPost2)
}

func TestCommandPostHandlerError(t *testing.T) {
	nH := 0
	nFPost1 := 0
	nFPost2 := 0
	nFErr := 0

	cmd := &Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nH++
		return nil, nil
	}

	fPost1 := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPost1++
		return nil, errors.New("err")
	}

	fPost2 := func(ctx context.Context, cmd *Command) (interface{}, error) {
		nFPost2++
		return nil, nil
	}

	fErr := func(ctx context.Context, cmd *Command, err error) {
		nFErr++
	}

	h := NewHandler()
	h.PostHandlers(fPost1, fPost2)
	h.ErrorHandlers(fErr)
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.handler(context.Background(), cmd)

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nFPost1)
	require.Equal(t, 0, nFPost2)
}
