package command_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestCommandHandler(t *testing.T) {
	spy := common.Spy()

	cmd := random.Command().
		ValidPermission("w1", "deleteWorkspace").
		Build()

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand(cmd.Function, f, command.WithPermission("deleteWorkspace"))

	resp, err := h.Handle(context.Background(), cmd)

	require.Nil(t, err)
	require.Nil(t, resp)
	require.True(t, spy.Has("f"))
}

func TestCommandNotFound(t *testing.T) {
	spy := common.Spy()

	cmd := &command.Command{
		Function: "xxxxxx",
	}

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.Handle(context.Background(), cmd)

	require.Error(t, errors.ErrCommandNotFound("xxxxxx"), err)
	require.Nil(t, resp)
	require.False(t, spy.Has("f"))
}

func TestCommandPreHandler(t *testing.T) {
	spy := common.Spy()

	cmd := &command.Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	fPre := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("fPre")
		return nil, nil
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		spy.Called("fErr")
	}

	h := command.NewHandler(&command.Config{})

	h.PreHandlers(fPre, fPre)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), cmd)

	require.Nil(t, err)
	require.Equal(t, nil, resp)
	require.Equal(t, 1, spy.Count("f"))
	require.Equal(t, 2, spy.Count("fPre"))
	require.Equal(t, 0, spy.Count("fErr"))
}

func TestCommandPreHandlerError(t *testing.T) {
	spy := common.Spy()

	cmd := &command.Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	fPre := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("fPre")
		return nil, nil
	}

	fPreErr := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("fPreErr")
		return nil, errors.ErrUnknown
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		spy.Called("fErr")
	}

	h := command.NewHandler(&command.Config{})

	h.PreHandlers(fPreErr, fPre)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), cmd)

	require.Equal(t, errors.ErrUnknown, err)
	require.Equal(t, nil, resp)
	require.Equal(t, 0, spy.Count("f"))
	require.Equal(t, 0, spy.Count("fPre"))
	require.Equal(t, 1, spy.Count("fPreErr"))
	require.Equal(t, 1, spy.Count("fErr"))
}

func TestCommandPreHandlerResult(t *testing.T) {
	spy := common.Spy()

	cmd := &command.Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	fPre := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("fPre")
		return nil, nil
	}

	fPre2 := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		spy.Called("fPre")
		return 1, nil
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		spy.Called("fErr")
	}

	h := command.NewHandler(&command.Config{})

	h.PreHandlers(fPre, fPre2)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), cmd)

	require.Nil(t, err)
	require.Equal(t, 1, resp)
	require.Equal(t, 0, spy.Count("f"))
	require.Equal(t, 2, spy.Count("fPre"))
	require.Equal(t, 0, spy.Count("fErr"))
}

func TestErrorHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	cmd := &command.Command{
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

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		require.False(t, checkFunc)
		checkFunc = true
		return nil, errors.ErrUnknown
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		errorFunc = true
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommandError", f, command.WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), cmd)

	require.Equal(t, errors.ErrUnknown, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestPanicHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	cmd := &command.Command{
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

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		require.False(t, checkFunc)
		checkFunc = true
		var e *command.Command
		_ = e.Args

		return nil, nil
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		errorFunc = true
		fmt.Println(err)
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommandError", f, command.WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), cmd)

	require.Equal(t, errors.ErrPanic, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestPanicStringHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	cmd := &command.Command{
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

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		require.False(t, checkFunc)
		checkFunc = true
		panic("hello")
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		errorFunc = true
		fmt.Println(err)
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommandError", f, command.WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), cmd)

	require.Equal(t, errors.ErrPanic, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestCommandPostHandler(t *testing.T) {
	nH := 0
	nFPost1 := 0
	nFPost2 := 0

	cmd := &command.Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		nH++
		return nil, nil
	}

	fPost1 := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		nFPost1++
		return 1, nil
	}

	fPost2 := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		nFPost2++
		return nil, nil
	}

	h := command.NewHandler(&command.Config{})
	h.PostHandlers(fPost1, fPost2)
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.Handle(context.Background(), cmd)

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

	cmd := &command.Command{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		nH++
		return nil, nil
	}

	fPost1 := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		nFPost1++
		return nil, errors.ErrUnknown
	}

	fPost2 := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
		nFPost2++
		return nil, nil
	}

	fErr := func(ctx context.Context, cmd *command.Command, err errors.Error) {
		nFErr++
	}

	h := command.NewHandler(&command.Config{})
	h.PostHandlers(fPost1, fPost2)
	h.ErrorHandlers(fErr)
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.Handle(context.Background(), cmd)

	require.Equal(t, errors.ErrUnknown, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nFPost1)
	require.Equal(t, 0, nFPost2)
}
