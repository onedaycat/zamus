package sagacmd

import (
    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
)

type State struct {
    Status     Status           `json:"status,omitempty"`
    Action     Action           `json:"action,omitempty"`
    Error      *errors.AppError `json:"error,omitempty"`
    Steps      []*Step          `json:"steps,omitempty"`
    Compensate bool             `json:"compensate,omitempty"`
    Data       interface{}      `json:"data,omitempty"`
    step       *Step
    defs       *StateDefinitions
    index      int
}

func newState(defs *StateDefinitions) *State {
    return &State{
        Steps: make([]*Step, 0, 10),
        index: -1,
        defs:  defs,
    }
}

func (s *State) startStep(stateName string, data interface{}) errors.Error {
    def := s.defs.GetState(stateName)
    if def == nil {
        return appErr.ErrNextStateNotFound(stateName).WithCaller().WithInput(stateName)
    }

    step := &Step{
        Name: def.Name,
        def:  def,
        data: data,
    }

    s.Steps = append(s.Steps, step)
    s.Status = INIT
    s.Action = NONE
    s.step = step
    s.Data = data
    s.index++

    return nil
}

func (s *State) nextStep() {
    statename := s.step.nextState
    def := s.defs.GetState(statename)
    if def == nil {
        err := appErr.ErrNextStateNotFound(statename).WithCaller().WithInput(statename)
        s.step.Fail(err)
        s.updateStep()
        return
    }

    step := &Step{
        Name: def.Name,
        def:  def,
        data: s.Data,
    }

    s.Steps = append(s.Steps, step)
    s.Status = INIT
    s.Action = NONE
    s.step = step
    s.index++
}

func (s *State) backStep() {
    s.index--
    if s.index < 0 {
        s.step.Action = END
        s.Action = END
        s.Status = COMPENSATED
        s.step.Status = COMPENSATED
        return
    }

    oldStep := s.Steps[s.index]

    def := s.defs.GetState(oldStep.Name)

    step := &Step{
        Name: oldStep.Name,
        def:  def,
        data: s.Data,
    }

    s.Steps = append(s.Steps, step)
    s.Status = INIT
    s.Action = NONE
    s.step = step
}

func (s *State) updateStep() {
    s.Action = s.step.Action
    s.Status = s.step.Status
    s.Data = s.step.data
    s.Error = s.step.StepError
}

func (s *State) Clear() {
    s.Status = INIT
    s.Action = NONE
    s.Error = nil
    s.Steps = s.Steps[:0]
    s.Compensate = false
    s.Data = nil
    s.step = nil
    s.index = -1
}

func (s *State) CurrentData() interface{} {
    return s.Data
}
