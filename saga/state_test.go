package saga

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/require"
)

func TestState_SetupFromResume(t *testing.T) {
	spy := common.Spy()
	compensate := func(ctx context.Context, data interface{}, stepAction StepAction) {
		spy.Called("compensate")
	}
	handler := func(ctx context.Context, data interface{}, stepAction StepAction) {
		spy.Called("handler")
	}
	defs := &StateDefinitions{
		Name: "ss",
		Definitions: []*StateDefinition{
			{
				Name:       "s1",
				Handler:    handler,
				Compensate: compensate,
			},
			{
				Name:       "s2",
				Handler:    handler,
				Compensate: compensate,
			},
		},
	}

	state := newState()
	state.Action = END
	state.Status = FAILED
	state.Data = []byte(`10`)
	state.Steps = []*Step{
		{
			Name:   "s1",
			Action: NEXT,
			Status: SUCCESS,
		},
		{
			Name:      "s2",
			Action:    END,
			Status:    FAILED,
			StepError: errors.DumbError,
		},
	}

	state.defs = defs
	state.setupFromResume(defs, 10)
	state.handler(nil, nil, nil)

	require.Equal(t, 10, state.data)
	require.Equal(t, 1, state.index)
	require.Equal(t, state.Steps[1], state.step)
	require.Equal(t, 10, state.Steps[1].data)
	require.Equal(t, 1, spy.Count("handler"))
	require.Equal(t, 0, spy.Count("compensate"))
	require.Equal(t, "s2", state.step.def.Name)

	state.Clear()
	spy.Reset()

	state = newState()
	state.Compensate = true
	state.Action = END
	state.Status = FAILED
	state.Data = []byte(`10`)
	state.Steps = []*Step{
		{
			Name:   "s1",
			Action: NEXT,
			Status: SUCCESS,
		},
		{
			Name:      "s2",
			Action:    END,
			Status:    FAILED,
			StepError: errors.DumbError,
		},
	}

	state.defs = defs
	state.setupFromResume(defs, 10)
	state.handler(nil, nil, nil)

	require.Equal(t, 10, state.data)
	require.Equal(t, 1, state.index)
	require.Equal(t, state.Steps[1], state.step)
	require.Equal(t, 10, state.Steps[1].data)
	require.Equal(t, 0, spy.Count("handler"))
	require.Equal(t, 1, spy.Count("compensate"))
	require.Equal(t, "s2", state.step.def.Name)
}

func TestState_UpdateState(t *testing.T) {
	compensate := func(ctx context.Context, data interface{}, stepAction StepAction) {}
	handler := func(ctx context.Context, data interface{}, stepAction StepAction) {}
	defs := &StateDefinitions{
		Name: "ss",
		Definitions: []*StateDefinition{
			{
				Name:       "s1",
				Handler:    handler,
				Compensate: compensate,
			},
			{
				Name:       "s2",
				Handler:    handler,
				Compensate: compensate,
			},
		},
	}

	state := newState()
	state.defs = defs

	err := state.newStep("s1", 10)
	require.NoError(t, err)

	state.step.Error(errors.DumbError)
	state.updateStep()

	require.Equal(t, RETRY, state.Action)
	require.Equal(t, ERROR, state.Status)
	require.Equal(t, errors.DumbError, state.Error)
	require.True(t, state.LastTime > 0)
	require.Equal(t, 10, state.data)

	state.step.End(20)
	state.updateStep()

	require.Equal(t, END, state.Action)
	require.Equal(t, SUCCESS, state.Status)
	require.Nil(t, state.Error)
	require.True(t, state.LastTime > 0)
	require.Equal(t, 20, state.data)
}

func TestState_NewStepNotFoundAndNextStep(t *testing.T) {
	compensate := func(ctx context.Context, data interface{}, stepAction StepAction) {}
	handler := func(ctx context.Context, data interface{}, stepAction StepAction) {}
	defs := &StateDefinitions{
		Name: "ss",
		Definitions: []*StateDefinition{
			{
				Name:       "s1",
				Handler:    handler,
				Compensate: compensate,
			},
			{
				Name:       "s2",
				Handler:    handler,
				Compensate: compensate,
			},
		},
	}

	state := newState()
	state.defs = defs

	err := state.newStep("s9", 10)
	require.Equal(t, appErr.ErrStateNotFound("s9").Error(), err.Error())
	// require.Equal(t, END, state.Action)
	// require.Equal(t, FAILED, state.Status)
	// require.Equal(t, appErr.ErrStateNotFound("s9"), state.Error)
	// require.True(t, state.LastTime > 0)
	// require.Equal(t, 10, state.data)

	err = state.newStep("s1", 20)
	require.NoError(t, err)

	state.step.Next("s2", 30)
	state.updateStep()
	require.Equal(t, NEXT, state.Action)
	require.Equal(t, SUCCESS, state.Status)
	require.Nil(t, state.Error)
	require.True(t, state.LastTime > 0)
	require.Equal(t, 30, state.data)
	require.Equal(t, "s1", state.step.Name)

	state.nextStep()
	require.Equal(t, NONE, state.Action)
	require.Equal(t, WAIT, state.Status)
	require.Nil(t, state.Error)
	require.Equal(t, 30, state.data)
	require.Equal(t, "s2", state.step.Name)
}

func TestState_BackStep(t *testing.T) {
	spy := common.Spy()
	compensate := func(ctx context.Context, data interface{}, stepAction StepAction) {
		spy.Called("compensate")
	}
	handler := func(ctx context.Context, data interface{}, stepAction StepAction) {
		spy.Called("handler")
	}
	defs := &StateDefinitions{
		Name: "ss",
		Definitions: []*StateDefinition{
			{
				Name:       "s1",
				Handler:    handler,
				Compensate: compensate,
			},
			{
				Name:       "s2",
				Handler:    handler,
				Compensate: compensate,
			},
			{
				Name:       "s3",
				Handler:    handler,
				Compensate: compensate,
			},
		},
	}

	state := newState()
	state.defs = defs

	err := state.newStep("s1", 10)
	require.NoError(t, err)

	state.step.Next("s2", 20)
	state.updateStep()
	state.nextStep()

	state.step.Next("s3", 30)
	state.updateStep()
	state.nextStep()

	state.step.Compensate(40, errors.DumbError)
	state.updateStep()
	require.True(t, state.Compensate)
	require.Equal(t, NEXT, state.Action)
	require.Equal(t, COMPENSATE, state.Status)
	require.Equal(t, errors.DumbError, state.Error)
	require.Equal(t, 40, state.data)
	require.Equal(t, "s3", state.step.Name)
	spy.Reset()

	ok := state.backStep()
	require.True(t, ok)
	require.Equal(t, NONE, state.Action)
	require.Equal(t, WAIT, state.Status)
	require.Nil(t, nil, state.Error)
	require.Equal(t, 40, state.data)
	require.Equal(t, "s2", state.step.Name)

	state.handler(nil, nil, nil)
	require.Equal(t, 1, spy.Count("compensate"))
	require.Equal(t, 0, spy.Count("handler"))

	ok = state.backStep()
	require.True(t, ok)
	require.Equal(t, NONE, state.Action)
	require.Equal(t, WAIT, state.Status)
	require.Nil(t, nil, state.Error)
	require.Equal(t, 40, state.data)
	require.Equal(t, "s1", state.step.Name)

	state.handler(nil, nil, nil)
	require.Equal(t, 2, spy.Count("compensate"))
	require.Equal(t, 0, spy.Count("handler"))

	state.step.Compensate(50)
	ok = state.backStep()
	require.False(t, ok)
	require.Equal(t, END, state.Action)
	require.Equal(t, WAIT, state.Status)
	require.Nil(t, nil, state.Error)
	require.Equal(t, 40, state.data)
	require.Equal(t, "s1", state.step.Name)
	require.Equal(t, -1, state.index)
}
