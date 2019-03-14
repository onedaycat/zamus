package query

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestParseBatchInvoke(t *testing.T) {

	testcases := []struct {
		payload  string
		expEvent *QueryReq
	}{
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), []byte(`[ {"namespace": "1"}, {"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 2, "pemKey", false, 0},
		},
		// no field
		{
			`[{"arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"", []byte(` {"arg1": "1"}`), []byte(`[ {"namespace": "1"}, {"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 2, "pemKey", false, 0},
		},
		// no args
		{
			`[{"function": "testField1","source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"testField1", nil, []byte(`[ {"namespace": "1"}, {"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 2, "pemKey", false, 0},
		},
		// no identity
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), []byte(`[ {"namespace": "1"}, {"namespace": "2"}]`), nil, 2, "pemKey", false, 0},
		},
		// missing source 1
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), []byte(`[ {"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 1, "pemKey", false, 0},
		},
		// missing source 2
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), []byte(`[ {"namespace": "1"}]`), &invoke.Identity{Sub: "xx"}, 1, "pemKey", false, 0},
		},
		// no source
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), nil, &invoke.Identity{Sub: "xx"}, 0, "pemKey", false, 0},
		},
	}

	for i, testcase := range testcases {
		req := &QueryReq{}
		err := req.UnmarshalRequest([]byte(testcase.payload))
		require.NoError(t, err)
		require.Equal(t, testcase.expEvent, req, i)
	}
}

func TestParseInvoke(t *testing.T) {
	testcases := []struct {
		payload  string
		expEvent *QueryReq
	}{
		{
			`{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), []byte(` {"namespace": "1"}`), &invoke.Identity{Sub: "xx"}, 0, "pemKey", false, 0},
		},
		// no field
		{
			`{"arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&QueryReq{"", []byte(` {"arg1": "1"}`), []byte(` {"namespace": "1"}`), &invoke.Identity{Sub: "xx"}, 0, "pemKey", false, 0},
		},
		// no args
		{
			`{"function": "testField1","source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&QueryReq{"testField1", nil, []byte(` {"namespace": "1"}`), &invoke.Identity{Sub: "xx"}, 0, "pemKey", false, 0},
		},
		// no identity
		{
			`{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"pemKey":"pemKey"}`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), []byte(` {"namespace": "1"}`), nil, 0, "pemKey", false, 0},
		},
		// no source
		{
			`{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&QueryReq{"testField1", []byte(` {"arg1": "1"}`), nil, &invoke.Identity{Sub: "xx"}, 0, "pemKey", false, 0},
		},
	}

	for i, testcase := range testcases {
		req := &QueryReq{}
		err := req.UnmarshalRequest([]byte(testcase.payload))
		require.NoError(t, err)
		require.Equal(t, testcase.expEvent, req, i)
	}
}

func TestQueryPermission(t *testing.T) {
	checkFunc := false

	f := func(ctx context.Context, query *QueryReq) (QueryResult, errors.Error) {
		checkFunc = true
		return newQueryResult(), nil
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("testHandlerCommandDenied", f, WithPermission("deleteWorkspace"))

	t.Run("Passed", func(t *testing.T) {
		checkFunc = false
		query := &QueryReq{
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

		resp, err := h.Handle(context.Background(), query)

		require.Nil(t, err)
		require.Equal(t, newQueryResult(), resp)
		require.True(t, checkFunc)
	})

	t.Run("Permission Denied", func(t *testing.T) {
		checkFunc = false
		query := &QueryReq{
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

		resp, err := h.Handle(context.Background(), query)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})

	t.Run("No Permission", func(t *testing.T) {
		checkFunc = false
		query := &QueryReq{
			Function:      "testHandlerCommandDenied",
			PermissionKey: "workspace_1",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{},
			},
		}

		resp, err := h.Handle(context.Background(), query)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})

	t.Run("No Identity", func(t *testing.T) {
		checkFunc = false
		query := &QueryReq{
			Function: "testHandlerCommandDenied",
		}

		resp, err := h.Handle(context.Background(), query)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})

	t.Run("No PermissionKey", func(t *testing.T) {
		checkFunc = false
		query := &QueryReq{
			Function: "testHandlerCommandDenied",
			Identity: &invoke.Identity{
				Claims: invoke.Claims{
					Permissions: invoke.Permissions{
						"workspace_1": "readWorkspace",
					},
				},
			},
		}

		resp, err := h.Handle(context.Background(), query)

		require.Equal(t, appErr.ErrPermissionDenied, err)
		require.Nil(t, resp)
		require.False(t, checkFunc)
	})
}
