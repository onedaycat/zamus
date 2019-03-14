package command

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func TestParseArguments(t *testing.T) {
	type arg struct {
		ID   string
		Name string
	}

	data := `{"function": "testField1","arguments": {"iD": "1", "NaMe":"hello"},"soUrCE": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`

	req := &CommandReq{}
	err := jsoniter.ConfigFastest.Unmarshal([]byte(data), req)
	require.NoError(t, err)

	a := &arg{}
	err = req.ParseArgs(a)
	require.NoError(t, err)
	require.Equal(t, &arg{
		ID:   "1",
		Name: "hello",
	}, a)
}

func TestParseSource(t *testing.T) {
	type source struct {
		Namespace string `json:"namespace"`
	}

	data := `{"function": "testField1","arguments": {"id": "1", "name":"hello"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`

	req := &CommandReq{}
	err := jsoniter.ConfigFastest.Unmarshal([]byte(data), req)
	require.NoError(t, err)

	s := &source{}
	err = req.ParseSource(s)
	require.NoError(t, err)
	require.Equal(t, &source{
		Namespace: "1",
	}, s)
}
