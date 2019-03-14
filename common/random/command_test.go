package random

import (
	"testing"

	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	var req *command.CommandReq

	req = CommandReq().
		Function("f1").
		Build()

	require.Equal(t, "f1", req.Function)

	arg := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	req = CommandReq().
		Arg(arg).
		Build()

	expArg := map[string]interface{}{}
	req.ParseArgs(&expArg)
	require.Equal(t, expArg, arg)

	source := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	req = CommandReq().
		Source(source).
		Build()
	expSource := map[string]interface{}{}
	req.ParseSource(&expSource)
	require.Equal(t, expSource, source)

	req = CommandReq().ValidPermission("w1", "open", "delete").Build()
	require.Equal(t, invoke.Permissions{
		"w1": "open,delete",
	}, req.Identity.Claims.Permissions)

	req = CommandReq().InvalidPermission().Build()
	require.NotNil(t, req.Identity.Claims.Permissions)
	require.Len(t, req.Identity.Claims.Permissions, 1)

	req = CommandReq().NoIdentity().Build()
	require.Nil(t, req.Identity)
	require.Empty(t, req.PermissionKey)

	reqByte := CommandReq().BuildJSON()
	require.NotNil(t, reqByte)
}
