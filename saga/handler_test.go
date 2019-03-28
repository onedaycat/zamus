package saga

import (
	"testing"
	"time"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/require"
)

func Test_Sentry(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("next")

	s.saga.ErrorHandlers(Sentry)

	_, err := s.saga.Invoke(s.ctx, []byte(`{"id":1}`))
	require.NoError(t, err)
	require.Equal(t, 0, s.handle.spy.Count("start"))
	require.Equal(t, 0, s.handle.spy.Count("s1"))
}

func Test_InvalidPayload(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("next")

	_, err := s.saga.Invoke(s.ctx, []byte(`{"id":1}`))
	require.NoError(t, err)
	require.Equal(t, 0, s.handle.spy.Count("start"))
	require.Equal(t, 0, s.handle.spy.Count("s1"))
}

func Test_Next3Handler(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("end")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     SUCCESS,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: false,
		handler:    s.defs.GetState("s3").Handler,
		Data:       []byte(`{"id":4}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s3",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
			{
				Name:      "s3",
				Status:    SUCCESS,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s3"),
				data:      &testdata{4},
			},
		},
		data:  &testdata{4},
		defs:  s.defs,
		index: 2,
	}

	expState.step = expState.Steps[2]

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 1, s.handle.spy.Count("s3"))
	require.Equal(t, 0, s.handle.spy.Count("s1comp"))
	require.Equal(t, 0, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, 2, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)
	require.Equal(t, expStateJSON, stateJSON)

	s.handle.spy.Reset()
	res, err = s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 1, s.handle.spy.Count("s3"))
	require.Equal(t, 0, s.handle.spy.Count("s1comp"))
	require.Equal(t, 0, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, 2, s.saga.state.index)

	expStateJSON, _ = common.MarshalJSON(expState)
	stateJSON, _ = common.MarshalJSON(s.saga.state)
	require.Equal(t, expStateJSON, stateJSON)
}

func Test_Next3AndCompensates(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("compensate").
		WithS2Compensate("compensate").
		WithS1Compensate("compensate")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     COMPENSATE,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: true,
		handler:    s.defs.GetState("s1").Compensate,
		Data:       []byte(`{"id":6}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s3",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
			{
				Name:      "s3",
				Status:    COMPENSATE,
				Action:    NEXT,
				NextState: "",
				Retried:   0,
				StepError: errors.DumbError,
				def:       s.defs.GetState("s3"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    COMPENSATE,
				Action:    NEXT,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{5},
			},
			{
				Name:      "s1",
				Status:    COMPENSATE,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{6},
			},
		},
		data:  &testdata{6},
		defs:  s.defs,
		index: -1,
	}

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 1, s.handle.spy.Count("s3"))
	require.Equal(t, 1, s.handle.spy.Count("s1comp"))
	require.Equal(t, 1, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, -1, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)

	require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_NextAndError(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("error").
		WithS2Compensate("compensate").
		WithS1Compensate("compensate")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     COMPENSATE,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: true,
		handler:    s.defs.GetState("s1").Compensate,
		Data:       []byte(`{"id":8}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s3",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
			{
				Name:      "s3",
				Status:    ERROR,
				Action:    RETRY,
				NextState: "",
				Retried:   2,
				StepError: errors.DumbError,
				def:       s.defs.GetState("s3"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    COMPENSATE,
				Action:    NEXT,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{5},
			},
			{
				Name:      "s1",
				Status:    COMPENSATE,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{6},
			},
		},
		data:  &testdata{8},
		defs:  s.defs,
		index: -1,
	}

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 3, s.handle.spy.Count("s3"))
	require.Equal(t, 1, s.handle.spy.Count("s1comp"))
	require.Equal(t, 1, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, -1, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)

	require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_Fail(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("fail")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     FAILED,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: false,
		handler:    s.defs.GetState("s1").Compensate,
		Data:       []byte(`{"id":3}`),
		Error:      errors.DumbError,
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    FAILED,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: errors.DumbError,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
		},
		data:  &testdata{3},
		defs:  s.defs,
		index: 1,
	}

	expState.step = expState.Steps[1]

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 0, s.handle.spy.Count("s3"))
	require.Equal(t, 0, s.handle.spy.Count("s1comp"))
	require.Equal(t, 0, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, 1, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)

	require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_End(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("end")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     SUCCESS,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: false,
		handler:    s.defs.GetState("s1").Compensate,
		Data:       []byte(`{"id":3}`),
		Error:      nil,
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
		},
		data:  &testdata{3},
		defs:  s.defs,
		index: 1,
	}

	expState.step = expState.Steps[1]

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 0, s.handle.spy.Count("s3"))
	require.Equal(t, 0, s.handle.spy.Count("s1comp"))
	require.Equal(t, 0, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, 1, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)

	require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_StartError(t *testing.T) {
	s := setupHandlerSuite().
		WithStartError()

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.Equal(t, appErr.ToLambdaError(errors.DumbError), err)
	require.Nil(t, res)
}

func Test_StartStepNotFound(t *testing.T) {
	s := setupHandlerSuite().
		WithStartStep("s00")

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.Equal(t, appErr.ToLambdaError(appErr.ErrStateNotFound("s00")), err)
	require.Nil(t, res)
}

func Test_NextStepNotFound(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS2NextStepNotfound()

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.NotNil(t, res)

	state, err := s.storage.Get(s.ctx, s.saga.state.ID)
	require.NoError(t, err)
	require.Equal(t, `{"id":3}`, string(state.Data))
	require.Equal(t, FAILED, state.Status)
	require.Equal(t, END, state.Action)
	require.Equal(t, appErr.ErrStateNotFound("s00").Error(), state.Error.Error())
}

func Test_NextOnCompensate(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("compensate").
		WithS1Compensate("next")

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.NotNil(t, res)

	state, err := s.storage.Get(s.ctx, s.saga.state.ID)
	require.NoError(t, err)
	require.Equal(t, `{"id":4}`, string(state.Data))
	require.Equal(t, FAILED, state.Status)
	require.Equal(t, END, state.Action)
	require.Equal(t, appErr.ErrNextOnCompensateNotAllowed, state.Error)
}

func Test_Panic(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS2Panic()

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.NotNil(t, res)

	state, err := s.storage.Get(s.ctx, s.saga.state.ID)
	require.NoError(t, err)
	require.Equal(t, `{"id":3}`, string(state.Data))
	require.Equal(t, FAILED, state.Status)
	require.Equal(t, END, state.Action)
	require.Equal(t, appErr.ErrPanic, state.Error)

	s = setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS2PanicString()

	res, err = s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.NotNil(t, res)

	state, err = s.storage.Get(s.ctx, s.saga.state.ID)
	require.NoError(t, err)
	require.Equal(t, `{"id":3}`, string(state.Data))
	require.Equal(t, FAILED, state.Status)
	require.Equal(t, END, state.Action)
	require.Equal(t, appErr.ErrPanic, state.Error)
}

func Test_Resume(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("end")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	saveState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     SUCCESS,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: false,
		Data:       []byte(`{"id":3}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
			},
			{
				Name:      "s2",
				Status:    FAILED,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: errors.DumbError,
			},
		},
	}
	xerr := s.storage.Save(s.ctx, saveState)
	require.NoError(t, xerr)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     SUCCESS,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: false,
		handler:    s.defs.GetState("s3").Handler,
		Data:       []byte(`{"id":5}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s3",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
			{
				Name:      "s3",
				Status:    SUCCESS,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s3"),
				data:      &testdata{4},
			},
		},
		data:  &testdata{4},
		defs:  s.defs,
		index: 2,
	}

	res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 0, s.handle.spy.Count("start"))
	require.Equal(t, 0, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 1, s.handle.spy.Count("s3"))
	require.Equal(t, 0, s.handle.spy.Count("s1comp"))
	require.Equal(t, 0, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, 2, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)

	require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumeCompensate(t *testing.T) {
	s := setupHandlerSuite().
		WithS1Handler("next").
		WithS2Handler("next").
		WithS3Handler("compensate").
		WithS2Compensate("compensate").
		WithS1Compensate("compensate")

	eid.FreezeID("state1")
	now := time.Now().UTC()
	clock.Freeze(now)

	saveState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     SUCCESS,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: true,
		Data:       []byte(`{"id":3}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s3",
				Retried:   0,
				StepError: nil,
			},
			{
				Name:      "s3",
				Status:    COMPENSATE,
				Action:    NEXT,
				NextState: "",
				Retried:   0,
				StepError: errors.DumbError,
			},
			{
				Name:      "s2",
				Status:    FAILED,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
			},
		},
	}
	xerr := s.storage.Save(s.ctx, saveState)
	require.NoError(t, xerr)

	expState := &State{
		ID:         "state1",
		Name:       "Test",
		Status:     COMPENSATE,
		Action:     END,
		Input:      []byte(`{"id":1}`),
		StartTime:  now.Unix(),
		LastTime:   now.Unix(),
		Compensate: true,
		handler:    s.defs.GetState("s1").Compensate,
		Data:       []byte(`{"id":6}`),
		Steps: []*Step{
			{
				Name:      "s1",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s2",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    SUCCESS,
				Action:    NEXT,
				NextState: "s3",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{4},
			},
			{
				Name:      "s3",
				Status:    COMPENSATE,
				Action:    NEXT,
				NextState: "",
				Retried:   0,
				StepError: errors.DumbError,
				def:       s.defs.GetState("s3"),
				data:      &testdata{4},
			},
			{
				Name:      "s2",
				Status:    COMPENSATE,
				Action:    NEXT,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s2"),
				data:      &testdata{5},
			},
			{
				Name:      "s1",
				Status:    COMPENSATE,
				Action:    END,
				NextState: "",
				Retried:   0,
				StepError: nil,
				def:       s.defs.GetState("s1"),
				data:      &testdata{6},
			},
		},
		data:  &testdata{6},
		defs:  s.defs,
		index: -1,
	}

	res, err := s.saga.Invoke(s.ctx, []byte(`{"input":{"id":1}}`))
	require.NoError(t, err)
	require.Equal(t, "state1", string(res))
	require.Equal(t, 1, s.handle.spy.Count("start"))
	require.Equal(t, 1, s.handle.spy.Count("s1"))
	require.Equal(t, 1, s.handle.spy.Count("s2"))
	require.Equal(t, 1, s.handle.spy.Count("s3"))
	require.Equal(t, 1, s.handle.spy.Count("s1comp"))
	require.Equal(t, 1, s.handle.spy.Count("s2comp"))
	require.Equal(t, 0, s.handle.spy.Count("s3comp"))
	require.Equal(t, -1, s.saga.state.index)

	expStateJSON, _ := common.MarshalJSON(expState)
	stateJSON, _ := common.MarshalJSON(s.saga.state)

	require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_ResumeNotFound(t *testing.T) {
	s := setupHandlerSuite()

	res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"10"}`))
	require.Equal(t, appErr.ToLambdaError(appErr.ErrUnableGetState), err)
	require.Nil(t, res)
}
