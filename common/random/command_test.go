package random

import (
	"encoding/json"
	"testing"

	"github.com/onedaycat/zamus/invoke"

	"github.com/onedaycat/zamus/command"
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
	json.Unmarshal(cmd.Args, &expArg)
	require.Equal(t, expArg, arg)

	source := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	cmd = Command().
		Source(source).
		Build()
	expSource := map[string]interface{}{}
	json.Unmarshal(cmd.Source, &expSource)
	require.Equal(t, expSource, source)

	cmd = Command().ValidPermission("w1", "open", "delete").Build()
	require.Equal(t, invoke.Permissions{
		"w1": "open,delete",
	}, cmd.Identity.Claims.Permissions)

	cmd = Command().InvalidPermission().Build()
	require.NotNil(t, cmd.Identity.Claims.Permissions)
	require.Len(t, cmd.Identity.Claims.Permissions, 1)
}
