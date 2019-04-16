package saga

import (
    "fmt"
    "testing"
    "time"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/common"
    "github.com/onedaycat/zamus/common/clock"
    "github.com/onedaycat/zamus/common/eid"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/stretchr/testify/require"
)

func Test_CompensateSuccess(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("back")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     COMPENSATED,
        Action:     END,
        Input:      []byte(`{"id":1}`),
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
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

func Test_CompensateFail(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("fail")

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
                Status:    FAILED,
                Action:    END,
                Retried:   0,
                StepError: errors.DumbError,
            },
        },
    }

    err := s.saga.Handle(s.ctx, &Request{Input: []byte(`{"id":1}`)})
    require.Equal(t, errors.DumbError, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)
    fmt.Println(string(stateJSON))

    require.Equal(t, string(expStateJSON), string(stateJSON))

    state, err := s.storage.Get(s.ctx, s.saga.state.Name, s.saga.state.ID)
    require.NoError(t, err)
    require.Equal(t, `{"id":6}`, string(state.Data))
    require.Equal(t, FAILED, state.Status)
    require.Equal(t, END, state.Action)
    require.Equal(t, errors.DumbError, state.Error)
}

func Test_CompensateError(t *testing.T) {
    s := setupHandlerSuite().
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS2Compensate("back").
        WithS1Compensate("error")

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
                Status:    FAILED,
                Action:    END,
                Retried:   2,
                StepError: errors.DumbError,
            },
        },
    }

    err := s.saga.Handle(s.ctx, &Request{Input: []byte(`{"id":1}`)})
    require.Equal(t, errors.DumbError, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 3, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))

    state, err := s.storage.Get(s.ctx, s.saga.state.Name, s.saga.state.ID)
    require.NoError(t, err)
    require.Equal(t, `{"id":8}`, string(state.Data))
    require.Equal(t, FAILED, state.Status)
    require.Equal(t, END, state.Action)
    require.Equal(t, errors.DumbError, state.Error)
}

func Test_PartialError(t *testing.T) {
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
        Data:       []byte(`{"id":9}`),
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

    err := s.saga.Handle(s.ctx, &Request{Input: []byte(`{"id":1}`)})
    require.Nil(t, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 3, s.handle.spy.Count("s3"))
    require.Equal(t, 1, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 1, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))
}

func Test_PartialCompensateError(t *testing.T) {
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

    err := s.saga.Handle(s.ctx, &Request{Input: []byte(`{"id":1}`)})
    require.Nil(t, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
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
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("compensate").
        WithS1Compensate("back")

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
        Compensate: true,
        Error:      appErr.ErrNoStateAction,
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
                Status:    FAILED,
                Action:    END,
                Retried:   0,
                StepError: appErr.ErrNoStateAction,
            },
        },
    }

    err := s.saga.Handle(s.ctx, &Request{Input: []byte(`{"id":1}`)})
    require.Equal(t, appErr.ErrNoStateAction, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 1, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))

    state, err := s.storage.Get(s.ctx, s.saga.state.Name, s.saga.state.ID)
    require.NoError(t, err)
    require.Equal(t, `{"id":5}`, string(state.Data))
    require.Equal(t, FAILED, state.Status)
    require.Equal(t, END, state.Action)
    require.Equal(t, appErr.ErrNoStateAction, state.Error)
}
