package memory

import (
    "context"
    "testing"
    "time"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/saga"
    "github.com/stretchr/testify/require"
)

func TestSaveAndGet(t *testing.T) {
    db := New()
    db.Clear()

    now := time.Now().UTC()

    state := &saga.State{}
    state.Clear()

    state.ID = "state1"
    state.Name = "Test"
    state.Status = saga.SUCCESS
    state.Action = saga.COMPENSATE
    state.Input = []byte(`{"id":1}`)
    state.StartTime = now.Unix()
    state.LastTime = now.Unix()
    state.Compensate = true
    state.Data = []byte(`{"id":6}`)
    state.Steps = []*saga.Step{
        {
            Name:      "s1",
            Status:    saga.SUCCESS,
            Action:    saga.NEXT,
            Retried:   0,
            StepError: nil,
        },
        {
            Name:      "s2",
            Status:    saga.SUCCESS,
            Action:    saga.NEXT,
            Retried:   0,
            StepError: nil,
        },
        {
            Name:      "s3",
            Status:    saga.ERROR,
            Action:    saga.COMPENSATE,
            Retried:   0,
            StepError: errors.DumbError,
        },
        {
            Name:      "s2",
            Status:    saga.SUCCESS,
            Action:    saga.BACK,
            Retried:   0,
            StepError: nil,
        },
        {
            Name:      "s1",
            Status:    saga.SUCCESS,
            Action:    saga.BACK,
            Retried:   0,
            StepError: nil,
        },
    }

    err := db.Save(context.Background(), state)
    require.NoError(t, err)

    getState, err := db.Get(context.Background(), state.Name, state.ID)
    require.NoError(t, err)
    require.Equal(t, state, getState)

    // NotFound
    getState, err = db.Get(context.Background(), state.Name, "xxxx")
    //noinspection GoNilness
    require.Equal(t, appErr.ErrStateNotFound.Error(), err.Error())
    require.Nil(t, getState)
}
