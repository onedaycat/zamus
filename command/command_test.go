package command_test

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

type CommandPermissionSuite struct {
	*common.SpyTest
	handler *command.Handler
}

func setupCommandPermission() *CommandPermissionSuite {
	s := &CommandPermissionSuite{}

	s.SpyTest = common.Spy()
	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		s.Called("f")
		return nil, nil
	}

	s.handler = command.NewHandler(&command.Config{SentryDNS: "test"})
	s.handler.RegisterCommand("testHandlerCommandDenied", f, command.WithPermission("deleteWorkspace"))

	return s
}

func Test_Command_Permission(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		s := setupCommandPermission()

		req := random.CommandReq().
			Function("testHandlerCommandDenied").
			ValidPermission("w1", "deleteWorkspace").
			Build()

		resp, err := s.handler.Handle(context.Background(), req)

		require.Nil(t, err)
		require.Nil(t, resp)
		require.True(t, s.Has("f"))
	})

	t.Run("Invalid", func(t *testing.T) {
		s := setupCommandPermission()

		req := random.CommandReq().
			Function("testHandlerCommandDenied").
			ValidPermission("w1", "readWorkspace").
			Build()

		resp, err := s.handler.Handle(context.Background(), req)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})

	t.Run("No Permission", func(t *testing.T) {
		s := setupCommandPermission()

		req := &command.CommandReq{
			Function:      "testHandlerCommandDenied",
			PermissionKey: "workspace_1",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{},
			},
		}

		resp, err := s.handler.Handle(context.Background(), req)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})

	t.Run("No Identity", func(t *testing.T) {
		s := setupCommandPermission()
		req := &command.CommandReq{
			Function: "testHandlerCommandDenied",
		}

		resp, err := s.handler.Handle(context.Background(), req)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})

	t.Run("No PermissionKey", func(t *testing.T) {
		s := setupCommandPermission()

		req := &command.CommandReq{
			Function: "testHandlerCommandDenied",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{
					Permissions: invoke.Permissions{
						"workspace_1": "readWorkspace",
					},
				},
			},
		}

		resp, err := s.handler.Handle(context.Background(), req)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})
}
