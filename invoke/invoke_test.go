package invoke

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseArguments(t *testing.T) {
	type arg struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	data := `{"function": "testField1","arguments": {"id": "1", "name":"hello"},"source": {"namespace": "1"},"identity": {"sub": "xx"},"pemKey":"pemKey"}`

	invoke := &InvokeEvent{}
	err := json.Unmarshal([]byte(data), invoke)
	require.NoError(t, err)

	a := &arg{}
	err = invoke.ParseArgs(a)
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

	invoke := &InvokeEvent{}
	err := json.Unmarshal([]byte(data), invoke)
	require.NoError(t, err)

	s := &source{}
	err = invoke.ParseSource(s)
	require.NoError(t, err)
	require.Equal(t, &source{
		Namespace: "1",
	}, s)
}
