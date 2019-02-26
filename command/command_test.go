package command

import (
	"context"
	"testing"

	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestCommandPermission(t *testing.T) {
	checkFunc := false

	f := func(ctx context.Context, cmd *Command) (interface{}, error) {
		checkFunc = true
		return nil, nil
	}

	h := NewHandler()
	h.RegisterCommand("testHandlerCommandDenied", f, WithPermission("deleteWorkspace"))

	t.Run("Passed", func(t *testing.T) {
		checkFunc = false
		cmd := &Command{
			Function:      "testHandlerCommandDenied",
			PermissionKey: "workspace_1",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{
					Permissions: invoke.Permissions{
						"workspace_1": "deleteWorkspace",
					},
				},
			},
		}

		resp, err := h.handler(context.Background(), cmd)

		require.Nil(t, err)
		require.Nil(t, resp)
		require.True(t, checkFunc)
	})

	t.Run("Permission Denied", func(t *testing.T) {
		checkFunc = false
		cmd := &Command{
			Function:      "testHandlerCommandDenied",
			PermissionKey: "workspace_1",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{
					Permissions: invoke.Permissions{
						"workspace_1": "readWorkspace",
					},
				},
			},
		}

		resp, err := h.handler(context.Background(), cmd)

		require.Equal(t, ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})

	t.Run("No Permission", func(t *testing.T) {
		checkFunc = false
		cmd := &Command{
			Function:      "testHandlerCommandDenied",
			PermissionKey: "workspace_1",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{},
			},
		}

		resp, err := h.handler(context.Background(), cmd)

		require.Equal(t, ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})

	t.Run("No Identity", func(t *testing.T) {
		checkFunc = false
		cmd := &Command{
			Function: "testHandlerCommandDenied",
		}

		resp, err := h.handler(context.Background(), cmd)

		require.Equal(t, ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})

	t.Run("No PermissionKey", func(t *testing.T) {
		checkFunc = false
		cmd := &Command{
			Function: "testHandlerCommandDenied",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{
					Permissions: invoke.Permissions{
						"workspace_1": "readWorkspace",
					},
				},
			},
		}

		resp, err := h.handler(context.Background(), cmd)

		require.Equal(t, ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})
}
