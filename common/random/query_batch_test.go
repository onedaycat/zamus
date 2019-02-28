package random

import (
	"encoding/json"
	"testing"

	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/query"

	"github.com/stretchr/testify/require"
)

func TestBatchQuery(t *testing.T) {
	var query *query.Query

	query = BatchQuery().
		Function("f1").
		Build()

	require.Equal(t, "f1", query.Function)

	arg := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	query = BatchQuery().
		Arg(arg).
		Build()

	expArg := map[string]interface{}{}
	json.Unmarshal(query.Args, &expArg)
	require.Equal(t, expArg, arg)

	source := map[string]interface{}{
		"id":   "id1",
		"name": "n1",
	}
	sources := []map[string]interface{}{source, source}

	query = BatchQuery().
		Sources(source, source).
		Build()
	expSources := []map[string]interface{}{}
	json.Unmarshal(query.Sources, &expSources)
	require.Equal(t, expSources, sources)
	require.Equal(t, 2, query.NBatchSources)

	query = BatchQuery().
		RandomSources(5).
		Build()
	require.Equal(t, 5, query.NBatchSources)
	require.NotNil(t, query.Sources)

	query = BatchQuery().ValidPermission("w1", "open", "delete").Build()
	require.Equal(t, invoke.Permissions{
		"w1": "open,delete",
	}, query.Identity.Claims.Permissions)

	query = BatchQuery().InvalidPermission().Build()
	require.NotNil(t, query.Identity.Claims.Permissions)
	require.Len(t, query.Identity.Claims.Permissions, 1)

	queryByte := BatchQuery().BuildJSON()
	require.NotNil(t, queryByte)
}
