package saga

import (
	"testing"
	"time"

	"github.com/onedaycat/errors"
	"github.com/stretchr/testify/require"
)

func TestStep_SleepDuration(t *testing.T) {
	step := &Step{
		Retried: 0,
		def: &StateDefinition{
			Retry:           3,
			IntervalSeconds: 10,
			BackoffRate:     2,
		},
	}

	ok := step.retry()
	require.True(t, ok)
	require.Equal(t, 1, step.Retried)
	require.Equal(t, 10*time.Second, step.sleepDuration())

	ok = step.retry()
	require.True(t, ok)
	require.Equal(t, 2, step.Retried)
	require.Equal(t, 20*time.Second, step.sleepDuration())

	ok = step.retry()
	require.True(t, ok)
	require.Equal(t, 3, step.Retried)
	require.Equal(t, 40*time.Second, step.sleepDuration())

	ok = step.retry()
	require.False(t, ok)
	require.Equal(t, 3, step.Retried)
	require.Equal(t, 40*time.Second, step.sleepDuration())
}

func TestStep_Next(t *testing.T) {
	step := &Step{
		def: &StateDefinition{},
	}

	step.Next("s1", 10)
	require.Equal(t, "s1", step.NextState)
	require.Equal(t, NEXT, step.Action)
	require.Equal(t, SUCCESS, step.Status)
	require.Equal(t, 10, step.data)
	require.Nil(t, step.StepError)
}

func TestStep_Compensate(t *testing.T) {
	step := &Step{
		def: &StateDefinition{},
	}

	step.Compensate(10, errors.DumbError)
	require.Equal(t, errors.DumbError, step.StepError)
	require.Equal(t, 10, step.data)
	require.Equal(t, NEXT, step.Action)
	require.Equal(t, COMPENSATE, step.Status)
}

func TestStep_CompensateNoError(t *testing.T) {
	step := &Step{
		def: &StateDefinition{},
	}

	step.Compensate(10)
	require.Nil(t, step.StepError)
	require.Equal(t, 10, step.data)
	require.Equal(t, NEXT, step.Action)
	require.Equal(t, COMPENSATE, step.Status)
}

func TestStep_Error(t *testing.T) {
	step := &Step{
		def: &StateDefinition{},
	}

	step.Error(errors.DumbError)
	require.Equal(t, errors.DumbError, step.StepError)
	require.Equal(t, RETRY, step.Action)
	require.Equal(t, ERROR, step.Status)
}

func TestStep_End(t *testing.T) {
	step := &Step{
		def: &StateDefinition{},
	}

	step.End(10)
	require.Equal(t, 10, step.data)
	require.Equal(t, END, step.Action)
	require.Equal(t, SUCCESS, step.Status)
	require.Nil(t, step.StepError)
}

func TestStep_Failed(t *testing.T) {
	step := &Step{
		def: &StateDefinition{},
	}

	step.Fail(errors.DumbError)
	require.Equal(t, errors.DumbError, step.StepError)
	require.Equal(t, END, step.Action)
	require.Equal(t, FAILED, step.Status)
}
