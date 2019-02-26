package random

import (
	"encoding/json"
	"testing"

	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/query"

	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	var query *query.Query

	query = Query().
		Function("f1").
		Build()

	require.Equal(t, "f1", query.Function)

	arg := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	query = Query().
		Arg(arg).
		Build()

	expArg := map[string]interface{}{}
	json.Unmarshal(query.Args, &expArg)
	require.Equal(t, expArg, arg)

	source := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	query = Query().
		Source(source).
		Build()
	expSource := map[string]interface{}{}
	json.Unmarshal(query.Sources, &expSource)
	require.Equal(t, expSource, source)

	query = Query().ValidPermission("w1", "open", "delete").Build()
	require.Equal(t, invoke.Permissions{
		"w1": "open,delete",
	}, query.Identity.Claims.Permissions)

	query = Query().InvalidPermission().Build()
	require.NotNil(t, query.Identity.Claims.Permissions)
	require.Len(t, query.Identity.Claims.Permissions, 1)
}
