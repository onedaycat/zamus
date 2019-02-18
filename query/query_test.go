package query

import (
	"encoding/json"
	"testing"

	"github.com/onedaycat/zamus/invoke"
	"github.com/stretchr/testify/require"
)

func TestParseBatchInvoke(t *testing.T) {

	testcases := []struct {
		payload  string
		expEvent *Query
	}{
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"},{"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 2, "pemKey"},
		},
		// no field
		{
			`[{"arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"},{"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 2, "pemKey"},
		},
		// no args
		{
			`[{"function": "testField1","source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"testField1", nil, []byte(`[{"namespace": "1"},{"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 2, "pemKey"},
		},
		// no identity
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"},{"namespace": "2"}]`), nil, 2, "pemKey"},
		},
		// missing source 1
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "2"}]`), &invoke.Identity{Sub: "xx"}, 1, "pemKey"},
		},
		// missing source 2
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"}]`), &invoke.Identity{Sub: "xx"}, 1, "pemKey"},
		},
		// no source
		{
			`[{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"},
			{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}]`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), nil, &invoke.Identity{Sub: "xx"}, 0, "pemKey"},
		},
	}

	for i, testcase := range testcases {
		req := &Query{}
		err := json.Unmarshal([]byte(testcase.payload), req)
		require.NoError(t, err)
		require.Equal(t, testcase.expEvent, req, i)
	}
}

func TestParseInvoke(t *testing.T) {
	testcases := []struct {
		payload  string
		expEvent *Query
	}{
		{
			`{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), []byte(`{"namespace": "1"}`), &invoke.Identity{Sub: "xx"}, 1, "pemKey"},
		},
		// no field
		{
			`{"arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&Query{"", []byte(`{"arg1": "1"}`), []byte(`{"namespace": "1"}`), &invoke.Identity{Sub: "xx"}, 1, "pemKey"},
		},
		// no args
		{
			`{"function": "testField1","source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&Query{"testField1", nil, []byte(`{"namespace": "1"}`), &invoke.Identity{Sub: "xx"}, 1, "pemKey"},
		},
		// no identity
		{
			`{"function": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"pemKey":"pemKey"}`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), []byte(`{"namespace": "1"}`), nil, 1, "pemKey"},
		},
		// no source
		{
			`{"function": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`,
			&Query{"testField1", []byte(`{"arg1": "1"}`), nil, &invoke.Identity{Sub: "xx"}, 1, "pemKey"},
		},
	}

	for i, testcase := range testcases {
		req := &Query{}
		err := json.Unmarshal([]byte(testcase.payload), req)
		require.NoError(t, err)
		require.Equal(t, testcase.expEvent, req, i)
	}
}
