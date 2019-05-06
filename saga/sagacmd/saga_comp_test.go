package sagacmd

import (
    "testing"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/stretchr/testify/require"
)

func Test_CompensateSuccess(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("back")

    expState := &State{
        Status:     COMPENSATED,
        Action:     END,
        Compensate: true,
        Data:       &testdata{ID: "6"},
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
                Status:    ERROR,
                Action:    COMPENSATE,
                StepError: errors.DumbError,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                StepError: nil,
            },
        },
    }

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.NoError(t, err)
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

func Test_CompensateFail(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("fail")

    expState := &State{
        Status:     FAILED,
        Action:     END,
        Compensate: true,
        Error:      errors.DumbError,
        Data:       &testdata{ID: "6"},
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
                Status:    ERROR,
                Action:    COMPENSATE,
                StepError: errors.DumbError,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                StepError: nil,
            },
            {
                Name:      "s1",
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
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_PartialCompensateError(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("partial_compensate").
        WithS3Compensate("back").
        WithS2Compensate("back").
        WithS1Compensate("back")

    expState := &State{

        Status:     COMPENSATED,
        Action:     END,
        Compensate: true,
        Error:      nil,
        Data:       &testdata{ID: "7"},
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
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                StepError: nil,
            },
        },
    }

    err := s.saga.Start(s.ctx, s.handle.startStep, s.data)
    require.Nil(t, err)
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 1, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_CompensateNoAction(t *testing.T) {
    s := setupHandlerSuite().
        WithData().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS1Compensate("back")

    expState := &State{

        Status:     FAILED,
        Action:     END,
        Compensate: true,
        Error:      appErr.ErrNoStateAction,
        Data:       &testdata{ID: "5"},
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
                Status:    ERROR,
                Action:    COMPENSATE,
                StepError: errors.DumbError,
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
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))
}
