package sagacmd

import (
    "testing"
    "time"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/internal/common/clock"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/stretchr/testify/require"
)

func Test_HandlerEnd(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("end")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        Status:     SUCCESS,
        Action:     END,
        Compensate: false,
        Data:       &testdata{ID: "4"},
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    END,
                StepError: nil,
            },
        },
    }

    expState.step = expState.Steps[2]

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.NoError(t, err)
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
    s.data.Reset()
    err = s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.NoError(t, err)
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

func Test_HandlerFail(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("fail")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        Status:     FAILED,
        Action:     END,
        Compensate: false,
        Data:       &testdata{ID: "4"},
        Error:      errors.DumbError,
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    FAILED,
                Action:    END,
                StepError: errors.DumbError,
            },
        },
    }

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Equal(t, errors.DumbError, err)
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 0, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_StartStepNotFound(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithStartStep("s00")

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Equal(t, appErr.ErrNextStateNotFound("s00").Error(), err.Error())
}

func Test_NextStepNotFound(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS2NextStepNotfound()

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Equal(t, appErr.ErrNextStateNotFound("s00").Error(), err.Error())
}

func Test_Panic(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS2Panic()

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Equal(t, appErr.ErrPanic, err)

    s = setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS2PanicString()

    err = s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Equal(t, appErr.ErrPanic, err)
}

func Test_HandlerNoAction(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next")

    expState := &State{
        Status:     FAILED,
        Action:     END,
        Compensate: false,
        Data:       &testdata{ID: "3"},
        Error:      appErr.ErrNoStateAction,
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
                StepError: appErr.ErrNoStateAction,
            },
        },
    }

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Equal(t, appErr.ErrNoStateAction, err)
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 0, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 0, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))
}
