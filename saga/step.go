package saga

import (
	"time"

	"github.com/onedaycat/errors"
)

type StepAction interface {
	Next(stateName string, data interface{})
	Compensate(err errors.Error, data interface{})
	Error(err errors.Error)
	End(data interface{})
	Fail(err errors.Error)
	PartialCompensate(err errors.Error, data interface{})
	PartialError(err errors.Error)
}

type CompensateAction interface {
	Back(data interface{})
	Error(err errors.Error)
	Fail(err errors.Error)
}

type Step struct {
	Name       string           `json:"name,omitempty"`
	Status     Status           `json:"status,omitempty"`
	Action     Action           `json:"action,omitempty"`
	Retried    int              `json:"retried,omitempty"`
	StepError  *errors.AppError `json:"error,omitempty"`
	def        *StateDefinition
	data       interface{}
	errPartial bool
	nextState  string
}

func (s *Step) Next(stateName string, data interface{}) {
	s.Action = NEXT
	s.Status = SUCCESS
	s.StepError = nil
	s.data = data
	s.nextState = stateName
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

func (s *Step) Error(err errors.Error) {
	s.Action = RETRY
	s.Status = ERROR
	s.StepError = err.(*errors.AppError)
}

func (s *Step) PartialError(err errors.Error) {
	s.Action = RETRY
	s.Status = ERROR
	s.StepError = err.(*errors.AppError)
	s.errPartial = true
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

func (s *Step) retry() bool {
	if s.def.Retry > s.Retried {
		s.Retried++
		return true
	}

	return false
}

func (s *Step) GetData() interface{} {
	return s.data
}

func (s *Step) sleepDuration() time.Duration {
	if s.Retried == 1 {
		return time.Second * time.Duration(s.def.IntervalSeconds)
	}
	return time.Second * time.Duration(s.def.IntervalSeconds*((s.Retried-1)*s.def.BackoffRate))
}
