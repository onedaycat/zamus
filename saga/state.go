package saga

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	appErr "github.com/onedaycat/zamus/errors"
)

type Status int
type Action int

const (
	WAIT Status = iota
	SUCCESS
	ERROR
	FAILED
	COMPENSATED
)

const (
	NONE Action = iota
	NEXT
	BACK
	RETRY
	END
	COMPENSATE
	PARTIAL_COMPENSATE
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
	Compensate      CompensateHandler
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

func (s *State) setupStart(input jsoniter.RawMessage) {
	s.ID = eid.GenerateID()
	s.StartTime = clock.Now().Unix()
	s.Input = input
}

func (s *State) setupFromResume(defs *StateDefinitions, data interface{}) {
	s.defs = defs
	s.step = s.Steps[len(s.Steps)-1]
	s.index = -1

	foundComp := false
	for i := range s.Steps {
		if !foundComp && s.Steps[i].Action == COMPENSATE {
			foundComp = true
			s.index++
		} else if !foundComp && s.Steps[i].Action == PARTIAL_COMPENSATE {
			foundComp = true
			s.index += 2
		} else {
			if foundComp {
				s.index--
			} else {
				s.index++
			}
		}
	}

	s.data = data
	s.step.def = s.defs.GetState(s.step.Name)
	s.step.data = data
	s.step.Action = NONE
	s.step.Status = WAIT
	s.Action = NONE
	s.Status = WAIT
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
	s.Status = WAIT
	s.Action = NONE
	s.step = step
	s.data = data
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
		data: s.data,
	}

	s.Steps = append(s.Steps, step)
	s.Status = WAIT
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
		data: s.data,
	}

	s.Steps = append(s.Steps, step)
	s.Status = WAIT
	s.Action = NONE
	s.step = step
}

func (s *State) updateStep() {
	s.Action = s.step.Action
	s.Status = s.step.Status
	s.data = s.step.data
	s.Error = s.step.StepError
	s.LastTime = clock.Now().Unix()
}

func (s *State) Clear() {
	s.ID = emptyStr
	s.Status = WAIT
	s.Action = NONE
	s.Input = nil
	s.Data = nil
	s.Error = nil
	s.Steps = s.Steps[:0]
	s.StartTime = 0
	s.LastTime = 0
	s.Compensate = false
	s.data = nil
	s.step = nil
	s.index = -1
}
