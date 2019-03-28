package saga

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/clock"
	appErr "github.com/onedaycat/zamus/errors"
)

type Status int
type Action int

const (
	WAIT Status = iota
	SUCCESS
	COMPENSATE
	ERROR
	FAILED
)

const (
	NONE Action = iota
	NEXT
	RETRY
	END
)

type StateDefinitions struct {
	Name        string
	Definitions []*StateDefinition
}

func (s *StateDefinitions) GetState(name string) *StateDefinition {
	for _, def := range s.Definitions {
		if def.Name == name {
			return def
		}
	}

	return nil
}

type StateDefinition struct {
	Name            string
	Retry           int
	IntervalSeconds int
	BackoffRate     int
	Handler         Handler
	Compensate      Handler
}

type State struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	Status     Status              `json:"status"`
	Action     Action              `json:"action"`
	Input      jsoniter.RawMessage `json:"input"`
	Data       jsoniter.RawMessage `json:"data"`
	Error      *errors.AppError    `json:"error"`
	Steps      []*Step             `json:"steps"`
	StartTime  int64               `json:"startTime"`
	LastTime   int64               `json:"lastTime"`
	Compensate bool                `json:"compensate"`
	handler    Handler
	data       interface{}
	step       *Step
	defs       *StateDefinitions
	index      int
}

func newState() *State {
	return &State{
		Steps: make([]*Step, 0, 30),
		index: -1,
	}
}

func (s *State) setupFromResume(defs *StateDefinitions, data interface{}) {
	s.defs = defs
	s.step = s.Steps[len(s.Steps)-1]
	s.index = len(s.Steps) - 1
	s.data = data
	s.step.def = s.defs.GetState(s.step.Name)
	s.step.data = data
	if s.Compensate {
		s.handler = s.step.def.Compensate
	} else {
		s.handler = s.step.def.Handler
	}
	s.step.Action = NONE
	s.step.Status = WAIT
	s.Action = NONE
	s.Status = WAIT
}

func (s *State) newStep(stateName string, data interface{}) errors.Error {
	def := s.defs.GetState(stateName)
	if def == nil {
		return appErr.ErrStateNotFound(stateName).WithCaller().WithInput(stateName)
		// step := &Step{
		// 	Name: stateName,
		// 	data: data,
		// }
		// step.Fail(appErr.ErrStateNotFound(stateName))
		// s.Steps = append(s.Steps, step)
		// s.step = step
		// s.handler = nil
		// s.index++
		// s.updateStep()

		// return s.Error
	}

	step := &Step{
		Name: def.Name,
		def:  def,
		data: data,
	}

	s.Steps = append(s.Steps, step)
	s.Status = WAIT
	s.Action = NONE
	s.step = step
	s.data = data
	s.handler = def.Handler
	s.index++

	return nil
}

func (s *State) nextStep() errors.Error {
	return s.newStep(s.step.NextState, s.step.data)
}

func (s *State) backStep() bool {
	s.index--
	if s.index < 0 {
		s.step.Action = END
		s.Action = END
		return false
	}

	oldStep := s.Steps[s.index]

	def := s.defs.GetState(oldStep.Name)

	step := &Step{
		Name: oldStep.Name,
		def:  def,
		data: s.data,
	}

	s.Steps = append(s.Steps, step)
	s.Status = WAIT
	s.Action = NONE
	s.step = step
	s.handler = def.Compensate

	return true
}

func (s *State) updateStep() {
	s.Action = s.step.Action
	s.Status = s.step.Status
	s.data = s.step.data
	s.Error = s.step.StepError
	s.LastTime = clock.Now().Unix()

	if s.step.Status == COMPENSATE {
		s.Compensate = true
	}
}

func (s *State) Clear() {
	s.ID = emptyStr
	s.Status = WAIT
	s.Action = NONE
	s.Data = nil
	s.Error = nil
	s.Steps = s.Steps[:0]
	s.StartTime = 0
	s.LastTime = 0
	s.Compensate = false
	s.handler = nil
	s.data = nil
	s.step = nil
	s.index = -1
}
