package common_test

import (
    "testing"

    "github.com/onedaycat/zamus/common"
    "github.com/onedaycat/zamus/common/random"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
    msgs := random.EventMsgs().RandomEventMsgs(10).Build()

    t.Run("MarshalAndUnmarshal", func(t *testing.T) {
        msgsByte, err := common.MarshalJSON(msgs)
        require.NoError(t, err)
        require.True(t, len(msgsByte) > 0)

        expMsgs := make([]*eventstore.EventMsg, 0, len(msgs))
        err = common.UnmarshalJSON(msgsByte, &expMsgs)
        require.NoError(t, err)
        require.Equal(t, msgs, expMsgs)
    })

    t.Run("MarshalAndUnmarshalEvent", func(t *testing.T) {
        evt := &domain.StockItemCreated{Id: "1"}
        evtAny, err := common.MarshalEvent(evt)
        require.NoError(t, err)
        require.NotNil(t, evtAny)

        mevt := &domain.StockItemCreated{}
        err = common.UnmarshalEvent(evtAny, mevt)
        require.NoError(t, err)
        require.Equal(t, evt, mevt)
    })

    t.Run("MarshalAndUnmarshalEventMsg", func(t *testing.T) {
        msgByte, err := common.MarshalEventMsg(msgs[0])
        require.NoError(t, err)
        require.True(t, len(msgByte) > 0)

        expMsgs := &eventstore.EventMsg{}
        err = common.UnmarshalEventMsg(msgByte, expMsgs)
        require.NoError(t, err)
        require.Equal(t, msgs[0], expMsgs)
    })
}
