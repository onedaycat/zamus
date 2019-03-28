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
	state.Status = saga.COMPENSATE
	state.Action = saga.END
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
			NextState: "s2",
			Retried:   0,
			StepError: nil,
		},
		{
			Name:      "s2",
			Status:    saga.SUCCESS,
			Action:    saga.NEXT,
			NextState: "s3",
			Retried:   0,
			StepError: nil,
		},
		{
			Name:      "s3",
			Status:    saga.COMPENSATE,
			Action:    saga.NEXT,
			NextState: "",
			Retried:   0,
			StepError: errors.DumbError,
		},
		{
			Name:      "s2",
			Status:    saga.COMPENSATE,
			Action:    saga.NEXT,
			NextState: "",
			Retried:   0,
			StepError: nil,
		},
		{
			Name:      "s1",
			Status:    saga.COMPENSATE,
			Action:    saga.END,
			NextState: "",
			Retried:   0,
			StepError: nil,
		},
	}

	err := db.Save(context.Background(), state)
	require.NoError(t, err)

	getState, err := db.Get(context.Background(), state.ID)
	require.NoError(t, err)
	require.Equal(t, state, getState)

	// NotFound
	getState, err = db.Get(context.Background(), "xxxx")
	require.Equal(t, appErr.ErrStateNotFound("xxxx").Error(), err.Error())
	require.Nil(t, getState)
}
