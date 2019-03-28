package saga

import (
	"time"

	"github.com/onedaycat/errors"
)

type StepAction interface {
	Next(stateName string, data interface{})
	Compensate(data interface{}, err ...errors.Error)
	Error(err errors.Error)
	End(data interface{})
	Fail(err errors.Error)
}

type Step struct {
	Name      string           `json:"name,omitempty"`
	Status    Status           `json:"status,omitempty"`
	Action    Action           `json:"action,omitempty"`
	NextState string           `json:"nextState,omitempty"`
	Retried   int              `json:"retried,omitempty"`
	StepError *errors.AppError `json:"error,omitempty"`
	def       *StateDefinition
	data      interface{}
}

func (s *Step) Next(stateName string, data interface{}) {
	s.data = data
	s.NextState = stateName
	s.Action = NEXT
	s.StepError = nil
	s.Status = SUCCESS
}

func (s *Step) Compensate(data interface{}, err ...errors.Error) {
	s.data = data
	if len(err) > 0 && err[0] != nil {
		s.StepError = err[0].(*errors.AppError)
	}
	s.Action = NEXT
	s.Status = COMPENSATE
}

func (s *Step) Error(err errors.Error) {
	s.StepError = err.(*errors.AppError)
	s.Action = RETRY
	s.Status = ERROR
}

func (s *Step) End(data interface{}) {
	s.data = data
	s.StepError = nil
	s.Action = END
	s.Status = SUCCESS
}

func (s *Step) Fail(err errors.Error) {
	s.StepError = err.(*errors.AppError)
	s.Action = END
	s.Status = FAILED
}

func (s *Step) retry() bool {
	if s.def.Retry > s.Retried {
		s.Retried++
		return true
	}

	return false
}

func (s *Step) sleepDuration() time.Duration {
	if s.Retried == 1 {
		return time.Second * time.Duration(s.def.IntervalSeconds)
	}
	return time.Second * time.Duration(s.def.IntervalSeconds*((s.Retried-1)*s.def.BackoffRate))
}
