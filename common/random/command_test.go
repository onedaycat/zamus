package random

import (
	"testing"

	jsoniter "github.com/json-iterator/go"

	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	var cmd *command.Command

	cmd = Command().
		Function("f1").
		Build()

	require.Equal(t, "f1", cmd.Function)

	arg := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	cmd = Command().
		Arg(arg).
		Build()

	expArg := map[string]interface{}{}
	jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(cmd.Args, &expArg)
	require.Equal(t, expArg, arg)

	source := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	cmd = Command().
		Source(source).
		Build()
	expSource := map[string]interface{}{}
	jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(cmd.Source, &expSource)
	require.Equal(t, expSource, source)

	cmd = Command().ValidPermission("w1", "open", "delete").Build()
	require.Equal(t, invoke.Permissions{
		"w1": "open,delete",
	}, cmd.Identity.Claims.Permissions)

	cmd = Command().InvalidPermission().Build()
	require.NotNil(t, cmd.Identity.Claims.Permissions)
	require.Len(t, cmd.Identity.Claims.Permissions, 1)

	cmd = Command().NoIdentity().Build()
	require.Nil(t, cmd.Identity)
	require.Empty(t, cmd.PermissionKey)

	cmdByte := Command().BuildJSON()
	require.NotNil(t, cmdByte)
}
