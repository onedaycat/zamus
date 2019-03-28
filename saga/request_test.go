package saga

import (
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/stretchr/testify/require"
)

func TestRequestFromInputAndResumeThenClear(t *testing.T) {
	var err errors.Error

	payload := `{
		"input": {"id":"123"},
		"resume": "1234"
	}`

	req := &Request{}
	err = common.UnmarshalJSON([]byte(payload), req)
	require.NoError(t, err)
	require.Equal(t, ` {"id":"123"}`, string(req.Input))
	require.Equal(t, "1234", req.Resume)

	req.clear()
	require.Nil(t, req.Input)
	require.Empty(t, req.Resume)
}

func TestRequestFromInputOnly(t *testing.T) {
	var err errors.Error

	payload := `{
		"input": {"id":"123"}
	}`

	req := &Request{}
	err = common.UnmarshalJSON([]byte(payload), req)
	require.NoError(t, err)
	require.Equal(t, ` {"id":"123"}`, string(req.Input))
	require.Empty(t, req.Resume)
}

func TestRequestSagaOnly(t *testing.T) {
	payload := `{
		"resume": "1234"
	}`

	req := &Request{}
	err := common.UnmarshalJSON([]byte(payload), req)
	require.NoError(t, err)
	require.Nil(t, req.Input)
	require.Equal(t, "1234", req.Resume)
}
