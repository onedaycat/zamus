package saga

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

func Test_ResumeNotFound(t *testing.T) {
    s := setupHandlerSuite()

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.Equal(t, appErr.ToLambdaError(appErr.ErrStateNotFound), err)
    require.Nil(t, res)
}

func Test_ResumeGetError(t *testing.T) {
    s := setupHandlerSuite().
        WithGetResumeError()

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.Equal(t, appErr.ToLambdaError(errors.DumbError), err)
    require.Nil(t, res)
}

func Test_ResumeParseError(t *testing.T) {
    s := setupHandlerSuite().
        WithGetResumeParseError()

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":3}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
                Retried:   0,
                StepError: errors.DumbError,
            },
        },
    }
    xerr := s.storage.Save(s.ctx, saveState)
    require.NoError(t, xerr)

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.Equal(t, appErr.ToLambdaError(errors.DumbError), err)
    require.Nil(t, res)
}

func Test_ResumeOnHandler(t *testing.T) {
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
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":3}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Data:       []byte(`{"id":5}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
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

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumeBeforeCompensate(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":3}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      nil,
        Data:       []byte(`{"id":7}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    COMPENSATE,
                Retried:   0,
                StepError: errors.DumbError,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumeAfterCompensate(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":3}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    COMPENSATE,
                Retried:   0,
                StepError: errors.DumbError,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      nil,
        Data:       []byte(`{"id":5}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    COMPENSATE,
                Retried:   0,
                StepError: errors.DumbError,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 0, s.handle.spy.Count("s2"))
    require.Equal(t, 0, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumePartialErrorBeforeComp(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("partial_error").
        WithS3Compensate("back").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":3}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      nil,
        Data:       []byte(`{"id":10}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                Retried:   2,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 3, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 1, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumePartialErrorAfterComp(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("partial_error").
        WithS3Compensate("back").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":8}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                Retried:   2,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      nil,
        Data:       []byte(`{"id":10}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                Retried:   2,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 0, s.handle.spy.Count("s2"))
    require.Equal(t, 0, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumePartialCompBeforeComp(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("partial_compensate").
        WithS3Compensate("back").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":3}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      nil,
        Data:       []byte(`{"id":8}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                Retried:   0,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 1, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumePartialCompAfterComp(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("partial_compensate").
        WithS3Compensate("back").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    saveState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      errors.DumbError,
        Data:       []byte(`{"id":6}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                Retried:   0,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    FAILED,
                Action:    END,
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
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Error:      nil,
        Data:       []byte(`{"id":8}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    ERROR,
                Action:    PARTIAL_COMPENSATE,
                Retried:   0,
                StepError: errors.DumbError,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    BACK,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s1",
                Status:    COMPENSATED,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 0, s.handle.spy.Count("s2"))
    require.Equal(t, 0, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}

func Test_ResumeFromStop(t *testing.T) {
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
        Status:     WAIT,
        Action:     STOP,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Data:       []byte(`{"id":3}`),
        Error:      nil,
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:    "s2",
                Status:  WAIT,
                Action:  STOP,
                Retried: 0,
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
        Error:      nil,
        Data:       []byte(`{"id":5}`),
        Steps: []*Step{
            {
                Name:      "s1",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s2",
                Status:    SUCCESS,
                Action:    NEXT,
                Retried:   0,
                StepError: nil,
            },
            {
                Name:      "s3",
                Status:    SUCCESS,
                Action:    END,
                Retried:   0,
                StepError: nil,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, []byte(`{"resume":"state1"}`))
    require.NoError(t, err)
    require.Equal(t, "state1", string(res))
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, expStateJSON, stateJSON)
}
