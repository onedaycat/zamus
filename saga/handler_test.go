package saga

import (
    "testing"
    "time"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/internal/common/clock"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/stretchr/testify/require"
)

func Test_InvalidPayload(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("next")

    _, err := s.saga.Invoke(s.ctx, s.msgByte[:len(s.msgByte)-1])
    require.Equal(t, appErr.ToLambdaError(appErr.ErrInvalidRequest), err)
    require.Equal(t, 0, s.handle.spy.Count("start"))
    require.Equal(t, 0, s.handle.spy.Count("s1"))
}

func Test_StartError(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithStartError()

    res, err := s.saga.Invoke(s.ctx, s.msgByte)
    require.Equal(t, appErr.ToLambdaError(errors.DumbError), err)
    require.Nil(t, res)
}

func Test_HandlerEnd(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
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
        EventMsg:   s.msg,
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Data:       []byte(`{"id":"4"}`),
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

    expState.step = expState.Steps[2]

    res, err := s.saga.Invoke(s.ctx, s.msgByte)
    require.NoError(t, err)
    require.Equal(t, "success", string(res))
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
    res, err = s.saga.Invoke(s.ctx, s.msgByte)
    require.NoError(t, err)
    require.Equal(t, "success", string(res))
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

func Test_HandlerError(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("error").
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
        EventMsg:   s.msg,
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: true,
        Data:       []byte(`{"id":"8"}`),
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
                Retried:   2,
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

    err := s.saga.Handle(s.ctx, s.req)
    require.NoError(t, err)
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

func Test_HandlerFail(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("fail")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        EventMsg:   s.msg,
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Data:       []byte(`{"id":"4"}`),
        Error:      errors.DumbError,
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
                Status:    FAILED,
                Action:    END,
                Retried:   0,
                StepError: errors.DumbError,
            },
        },
    }

    err := s.saga.Handle(s.ctx, s.req)
    require.Equal(t, errors.DumbError, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 0, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))

    dlqMsg, err := s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err := ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"4"}`, string(getState.Data))
    require.Equal(t, FAILED, getState.Status)
    require.Equal(t, END, getState.Action)
    require.Equal(t, errors.DumbError, getState.Error)
}

func Test_HandlerFailWithReturnResponse(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS3Handler("fail").
        WithReutnFailedOnError()

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        EventMsg:   s.msg,
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Data:       []byte(`{"id":"4"}`),
        Error:      errors.DumbError,
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
                Status:    FAILED,
                Action:    END,
                Retried:   0,
                StepError: errors.DumbError,
            },
        },
    }

    res, err := s.saga.Invoke(s.ctx, s.msgByte)
    require.Equal(t, appErr.ToLambdaError(errors.DumbError), err)
    require.Nil(t, res)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 1, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 0, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))

    dlqMsg, err := s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err := ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"4"}`, string(getState.Data))
    require.Equal(t, FAILED, getState.Status)
    require.Equal(t, END, getState.Action)
    require.Equal(t, errors.DumbError, getState.Error)
}

func Test_StartStepNotFound(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithStartStep("s00")

    res, err := s.saga.Invoke(s.ctx, s.msgByte)
    require.Equal(t, appErr.ToLambdaError(appErr.ErrNextStateNotFound("s00")), err)
    require.Nil(t, res)
}

func Test_NextStepNotFound(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS2NextStepNotfound()

    res, err := s.saga.Invoke(s.ctx, s.msgByte)
    require.NoError(t, err)
    require.NotNil(t, res)

    dlqMsg, err := s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err := ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"3"}`, string(getState.Data))
    require.Equal(t, FAILED, getState.Status)
    require.Equal(t, END, getState.Action)
    require.Equal(t, appErr.ErrNextStateNotFound("s00").Error(), getState.Error.Error())
}

func Test_Panic(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS2Panic()

    eid.FreezeID("1")

    res, err := s.saga.Invoke(s.ctx, s.msgByte)
    require.NoError(t, err)
    require.NotNil(t, res)

    dlqMsg, err := s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err := ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"3"}`, string(getState.Data))
    require.Equal(t, FAILED, getState.Status)
    require.Equal(t, END, getState.Action)
    require.Equal(t, appErr.ErrPanic, getState.Error)

    s = setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("next").
        WithS2PanicString()

    res, err = s.saga.Invoke(s.ctx, s.msgByte)
    require.NoError(t, err)
    require.NotNil(t, res)

    dlqMsg, err = s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err = ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"3"}`, string(getState.Data))
    require.Equal(t, FAILED, getState.Status)
    require.Equal(t, END, getState.Action)
    require.Equal(t, appErr.ErrPanic, getState.Error)
}

func Test_HandlerNoAction(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     FAILED,
        Action:     END,
        EventMsg:   s.msg,
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Data:       []byte(`{"id":"3"}`),
        Error:      appErr.ErrNoStateAction,
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
                StepError: appErr.ErrNoStateAction,
            },
        },
    }

    err := s.saga.Handle(s.ctx, s.req)
    require.Equal(t, appErr.ErrNoStateAction, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 0, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 0, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))

    dlqMsg, err := s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err := ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"3"}`, string(getState.Data))
    require.Equal(t, FAILED, getState.Status)
    require.Equal(t, END, getState.Action)
    require.Equal(t, appErr.ErrNoStateAction, getState.Error)
}

func Test_HandlerStop(t *testing.T) {
    s := setupHandlerSuite().
        WithEventMsg("1").
        WithS1Handler("next").
        WithS2Handler("stop")

    eid.FreezeID("state1")
    now := time.Now().UTC()
    clock.Freeze(now)

    expState := &State{
        ID:         "state1",
        Name:       "Test",
        Status:     WAIT,
        Action:     STOP,
        EventMsg:   s.msg,
        StartTime:  now.Unix(),
        LastTime:   now.Unix(),
        Compensate: false,
        Data:       []byte(`{"id":"3"}`),
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

    err := s.saga.Handle(s.ctx, s.req)
    require.NoError(t, err)
    require.Equal(t, 1, s.handle.spy.Count("start"))
    require.Equal(t, 1, s.handle.spy.Count("s1"))
    require.Equal(t, 1, s.handle.spy.Count("s2"))
    require.Equal(t, 0, s.handle.spy.Count("s3"))
    require.Equal(t, 0, s.handle.spy.Count("s1comp"))
    require.Equal(t, 0, s.handle.spy.Count("s2comp"))
    require.Equal(t, 0, s.handle.spy.Count("s3comp"))

    expStateJSON, _ := common.MarshalJSON(expState)
    stateJSON, _ := common.MarshalJSON(s.saga.state)

    require.Equal(t, string(expStateJSON), string(stateJSON))

    dlqMsg, err := s.storage.Get(s.ctx, dlq.Saga, s.saga.state.ID)
    require.NoError(t, err)
    getState, err := ParseStateFromDLQMsg(dlqMsg)
    require.NoError(t, err)
    require.Equal(t, `{"id":"3"}`, string(getState.Data))
    require.Equal(t, WAIT, getState.Status)
    require.Equal(t, STOP, getState.Action)
    require.Nil(t, getState.Error)
}
