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
	f := func(ctx context.Context, cmd *command.Command) (interface{}, errors.Error) {
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

		cmd := random.Command().
			Function("testHandlerCommandDenied").
			ValidPermission("w1", "deleteWorkspace").
			Build()

		resp, err := s.handler.Handle(context.Background(), cmd)

		require.Nil(t, err)
		require.Nil(t, resp)
		require.True(t, s.Has("f"))
	})

	t.Run("Invalid", func(t *testing.T) {
		s := setupCommandPermission()

		cmd := random.Command().
			Function("testHandlerCommandDenied").
			ValidPermission("w1", "readWorkspace").
			Build()

		resp, err := s.handler.Handle(context.Background(), cmd)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})

	t.Run("No Permission", func(t *testing.T) {
		s := setupCommandPermission()

		cmd := &command.Command{
			Function:      "testHandlerCommandDenied",
			PermissionKey: "workspace_1",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{},
			},
		}

		resp, err := s.handler.Handle(context.Background(), cmd)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})

	t.Run("No Identity", func(t *testing.T) {
		s := setupCommandPermission()
		cmd := &command.Command{
			Function: "testHandlerCommandDenied",
		}

		resp, err := s.handler.Handle(context.Background(), cmd)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})

	t.Run("No PermissionKey", func(t *testing.T) {
		s := setupCommandPermission()

		cmd := &command.Command{
			Function: "testHandlerCommandDenied",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{
					Permissions: invoke.Permissions{
						"workspace_1": "readWorkspace",
					},
				},
			},
		}

		resp, err := s.handler.Handle(context.Background(), cmd)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, s.Has("f"))
	})
}
