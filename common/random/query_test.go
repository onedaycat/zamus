package random

import (
	"testing"

	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/query"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	var req *query.QueryReq

	req = QueryReq().
		Function("f1").
		Build()

	require.Equal(t, "f1", req.Function)

	arg := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	req = QueryReq().
		Arg(arg).
		Build()

	expArg := map[string]interface{}{}
	req.ParseArgs(&expArg)
	require.Equal(t, expArg, arg)

	source := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	req = QueryReq().
		Source(source).
		Build()
	expSource := map[string]interface{}{}
	req.ParseSource(&expSource)
	require.Equal(t, expSource, source)

	req = QueryReq().ValidPermission("w1", "open", "delete").Build()
	require.Equal(t, invoke.Permissions{
		"w1": "open,delete",
	}, req.Identity.Claims.Permissions)

	req = QueryReq().InvalidPermission().Build()
	require.NotNil(t, req.Identity.Claims.Permissions)
	require.Len(t, req.Identity.Claims.Permissions, 1)

	req = QueryReq().NoIdentity().Build()
	require.Nil(t, req.Identity)
	require.Empty(t, req.PermissionKey)

	queryByte := QueryReq().BuildJSON()
	require.NotNil(t, queryByte)
}
