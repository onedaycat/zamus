package common_test

import (
	"testing"

	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
	msgs := random.EventMsgs().RandomEventMsgs(10).Build()

	t.Run("MarshalAndUnmarshal", func(t *testing.T) {
		msgsByte, err := common.MarshalJSON(msgs)
		require.NoError(t, err)
		require.True(t, len(msgsByte) > 0)

		expMsgs := make([]*eventstore.EventMsg, 0, len(msgs))
		common.UnmarshalJSON(msgsByte, &expMsgs)
		require.NoError(t, err)
		require.Equal(t, msgs, expMsgs)
	})

	t.Run("MarshalAndUnmarshalWithSnappy", func(t *testing.T) {
		msgsByte, err := common.MarshalJSONSnappy(msgs)
		require.NoError(t, err)
		require.True(t, len(msgsByte) > 0)

		expMsgs := make([]*eventstore.EventMsg, 0, len(msgs))
		common.UnmarshalJSONSnappy(msgsByte, &expMsgs)
		require.NoError(t, err)
		require.Equal(t, msgs, expMsgs)
	})
}
