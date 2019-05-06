package sagacmd

import (
    "context"
    "fmt"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
)

type StepHandler func(ctx context.Context, data interface{}, action StepAction)
type CompensateHandler func(ctx context.Context, data interface{}, action CompensateAction)
type ErrorHandler = func(ctx context.Context, state *State, err errors.Error)

type SagaHandle interface {
    StateDefinitions() *StateDefinitions
}

//go:generate mockery -name=Saga
type Saga interface {
    Start(ctx context.Context, stateName string, data interface{}) errors.Error
    GetData() interface{}
    GetState() *State
}

type SagaCmd struct {
    state   *State
    handler SagaHandle
}

func New(handler SagaHandle) *SagaCmd {
    defs := handler.StateDefinitions()
    return &SagaCmd{
        handler: handler,
        state:   newState(defs),
    }
}

func (s *SagaCmd) Start(ctx context.Context, stateName string, data interface{}) errors.Error {
    s.state.Clear()

    err := s.state.startStep(stateName, data)
    if err != nil {
        return err
    }

    if len(s.state.defs.Definitions) == 0 || len(s.state.Steps) == 0 {
        return nil
    }

    for s.state.Action != END {
        if s.state.Compensate {
            s.doCompensate(ctx)
        } else {
            s.doHandler(ctx)
            if s.state.Status == WAIT {
                break
            }
        }
    }

    if s.state.Error != nil {
        return s.state.Error
    }

    return nil
}

func (s *SagaCmd) doHandler(ctx context.Context) {
    var err errors.Error
    defer s.recovery(ctx, &err)

    s.state.step.def.StepHandler(ctx, s.state.Data, s.state.step)

    if s.state.step.Status == INIT {
        s.state.step.Fail(appErr.ErrNoStateAction)
    }
    s.state.updateStep()

    switch s.state.Status {
    case SUCCESS:
        if s.state.Action == END {
            break
        }
        s.state.nextStep()
    case ERROR:
        switch s.state.Action {
        case PARTIAL_COMPENSATE:
            s.state.index++
            s.state.Compensate = true
            s.state.backStep()
        case COMPENSATE:
            s.state.Compensate = true
            s.state.backStep()
        }
    }
}

func (s *SagaCmd) doCompensate(ctx context.Context) {
    var err errors.Error
    defer s.recovery(ctx, &err)

    s.state.step.def.CompensateHandler(ctx, s.state.Data, s.state.step)

    if s.state.step.Status == INIT {
        s.state.step.Fail(appErr.ErrNoStateAction)
    }
    s.state.updateStep()

    switch s.state.Status {
    case SUCCESS:
        s.state.backStep()
    }
}

func (s *SagaCmd) recovery(ctx context.Context, err *errors.Error) {
    if r := recover(); r != nil {
        switch cause := r.(type) {
        case error:
            *err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(s.state)
            s.state.step.Fail(*err)
            s.state.updateStep()
        default:
            *err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(s.state)
            s.state.step.Fail(*err)
            s.state.updateStep()
        }
    }
}

func (s *SagaCmd) GetState() *State {
    return s.state
}

func (s *SagaCmd) GetData() interface{} {
    return s.state.Data
}
