package sagacmd

import (
    "github.com/onedaycat/errors"
)

type Status int
type Action int

//noinspection ALL
const (
    INIT Status = iota
    SUCCESS
    ERROR
    FAILED
    COMPENSATED
    WAIT
)

//noinspection ALL
const (
    NONE Action = iota
    NEXT
    BACK
    END
    COMPENSATE
    PARTIAL_COMPENSATE
)

type StepAction interface {
    Next(stateName string, data interface{})
    Compensate(err errors.Error, data interface{})
    Fail(err errors.Error)
    PartialCompensate(err errors.Error, data interface{})
    End(data interface{})
}

type CompensateAction interface {
    Back(data interface{})
    Fail(err errors.Error)
}

type Step struct {
    Name      string
    Status    Status
    Action    Action
    StepError *errors.AppError
    NextState string `json:"-"`
    def       *StateDefinition
    data      interface{}
}

func (s *Step) Next(stateName string, data interface{}) {
    s.Action = NEXT
    s.Status = SUCCESS
    s.StepError = nil
    s.data = data
    s.NextState = stateName
}

func (s *Step) Back(data interface{}) {
    s.Action = BACK
    s.Status = SUCCESS
    s.StepError = nil
    s.data = data
}

func (s *Step) Compensate(err errors.Error, data interface{}) {
    s.Action = COMPENSATE
    s.Status = ERROR
    s.StepError = err.(*errors.AppError)
    s.data = data
}

func (s *Step) PartialCompensate(err errors.Error, data interface{}) {
    s.Action = PARTIAL_COMPENSATE
    s.Status = ERROR
    s.StepError = err.(*errors.AppError)
    s.data = data
}

func (s *Step) End(data interface{}) {
    s.Action = END
    s.Status = SUCCESS
    s.StepError = nil
    s.data = data
}

func (s *Step) Fail(err errors.Error) {
    s.Action = END
    s.Status = FAILED
    s.StepError = err.(*errors.AppError)
}

func (s *Step) GetData() interface{} {
    return s.data
}
