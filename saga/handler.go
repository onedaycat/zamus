package saga

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/zamuscontext"
)

const (
	emptyStr = ""
)

type SagaHandle interface {
	// StateDefinitions is a definition of handlers
	// If do not define these, Saga won't work.
	StateDefinitions() *StateDefinitions
	// Start will be run when the request payload has input field.
	// and return the start state name which defined from StateDefinitions() with parsed data to be yours format.
	// If error, it will response to the requester
	Start(ctx context.Context, input Payload) (string, interface{}, errors.Error)
	// ParseData will be run when the request payload has resume field.
	// Saga load data from the storage and must to parse back to yours data format
	ParseData(dataPayload Payload) (interface{}, errors.Error)
}

type StepHandler func(ctx context.Context, data interface{}, action StepAction)
type CompensateHandler func(ctx context.Context, data interface{}, action CompensateAction)
type ErrorHandler = func(ctx context.Context, state *State, err errors.Error)
type Payload jsoniter.RawMessage

func (p Payload) Unmarshal(v interface{}) errors.Error {
	if err := common.UnmarshalJSON(p, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(p)
	}

	return nil
}

type Config struct {
	AppStage      string
	Service       string
	Version       string
	SentryRelease string
	SentryDNS     string
	EnableTrace   bool
}

type Saga struct {
	errorhandlers []ErrorHandler
	zcctx         *zamuscontext.ZamusContext
	handle        SagaHandle
	req           *Request
	storage       Storage
	state         *State
}

func New(handle SagaHandle, storage Storage, config *Config) *Saga {
	s := &Saga{
		handle:  handle,
		storage: storage,
		state:   newState(),
		req:     &Request{},
	}

	s.state.defs = handle.StateDefinitions()
	s.state.Name = s.state.defs.Name

	if config.EnableTrace {
		tracer.Enable = config.EnableTrace
		s.ErrorHandlers(TraceError)
	}

	if config.SentryDNS != "" {
		sentry.SetDSN(config.SentryDNS)
		sentry.SetOptions(
			sentry.WithEnv(config.AppStage),
			sentry.WithServerName(lambdacontext.FunctionName),
			sentry.WithServiceName(config.Service),
			sentry.WithRelease(config.Service+"@"+config.Version),
			sentry.WithVersion(config.Version),
			sentry.WithTags(sentry.Tags{
				{"lambdaVersion", lambdacontext.FunctionVersion},
			}),
		)
		s.ErrorHandlers(Sentry)
	}

	return s
}

func (s *Saga) ErrorHandlers(handlers ...ErrorHandler) {
	s.errorhandlers = append(s.errorhandlers, handlers...)
}

func (s *Saga) Handle(ctx context.Context, req *Request) errors.Error {
	zmctx := zamuscontext.NewContext(ctx, s.zcctx)
	s.state.Clear()
	s.req = req

	if req.Input != nil {
		if err := s.doStart(zmctx); err != nil {
			return err
		}
	} else if req.Resume != emptyStr {
		if err := s.doResume(zmctx); err != nil {
			return err
		}
	} else {
		return appErr.ErrInvalidRequest.WithCaller().WithInput(req)
	}

	if len(s.state.defs.Definitions) == 0 || len(s.state.Steps) == 0 {
		return nil
	}

	for s.state.Action != END {
		if s.state.Compensate {
			s.doCompensate(zmctx)
		} else {
			s.doHandler(zmctx)
		}
	}

	s.state.Data, _ = common.MarshalJSON(s.state.data)

	if s.state.Error != nil {
		return s.state.Error
	}

	return nil
}

func (s *Saga) doResume(ctx context.Context) (err errors.Error) {
	var state *State
	defer s.recovery(ctx, &err)
	state, err = s.storage.Get(ctx, s.req.Resume)
	if err != nil {
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, err)
		}

		return err
	}
	s.state = state
	var data interface{}
	data, err = s.handle.ParseData(Payload(s.state.Data))
	if err != nil {
		s.state.Error = nil
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, err)
		}

		return err
	}

	s.state.setupFromResume(s.handle.StateDefinitions(), data)

	return nil
}

func (s *Saga) doStart(ctx context.Context) (err errors.Error) {
	defer s.recovery(ctx, &err)
	var stateName string
	var data interface{}

	stateName, data, err = s.handle.Start(ctx, Payload(s.req.Input))
	if err != nil {
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, err)
		}
		return err
	}

	s.state.setupStart(s.req.Input)
	err = s.state.startStep(stateName, data)
	if err != nil {
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, err)
		}

		return err
	}

	return nil
}

func (s *Saga) doHandler(ctx context.Context) {
	var err errors.Error
	defer s.recovery(ctx, &err)

	s.state.step.def.StepHandler(ctx, s.state.data, s.state.step)

	if s.state.step.Status == WAIT {
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
		case RETRY:
			if s.state.step.retry() {
				time.Sleep(s.state.step.sleepDuration())
				break
			}
			s.runErrorHandler(ctx, err)
			if s.state.step.errPartial {
				s.state.index++
				s.state.step.PartialCompensate(s.state.Error, s.state.data)
				s.state.updateStep()
			} else {
				s.state.step.Compensate(s.state.Error, s.state.data)
				s.state.updateStep()
			}
			s.state.Compensate = true
			s.state.backStep()
		case PARTIAL_COMPENSATE:
			s.state.index++
			s.state.Compensate = true
			s.state.backStep()
		case COMPENSATE:
			s.state.Compensate = true
			s.state.backStep()
		}
	}

	if s.state.Status == FAILED {
		s.runErrorHandler(ctx, s.state.Error)
		s.save(ctx)
	}
}

func (s *Saga) doCompensate(ctx context.Context) {
	var err errors.Error
	defer s.recovery(ctx, &err)

	s.state.step.def.CompensateHandler(ctx, s.state.data, s.state.step)

	if s.state.step.Status == WAIT {
		s.state.step.Fail(appErr.ErrNoStateAction)
	}
	s.state.updateStep()

	switch s.state.Status {
	case SUCCESS:
		s.state.backStep()
	case ERROR:
		if s.state.step.retry() {
			time.Sleep(s.state.step.sleepDuration())
			break
		}
		s.runErrorHandler(ctx, err)
		s.state.step.Fail(s.state.Error)
		s.state.updateStep()
	}

	if s.state.Status == FAILED {
		s.runErrorHandler(ctx, s.state.Error)
		s.save(ctx)
	}
}

func (s *Saga) save(ctx context.Context) {
	if err := s.storage.Save(ctx, s.state); err != nil {
		s.runErrorHandler(ctx, err)
	}
}

func (s *Saga) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	s.req.clear()

	if err := common.UnmarshalJSON(payload, s.req); err != nil {
		return nil, appErr.ToLambdaError(err)
	}

	if err := s.Handle(ctx, s.req); err != nil {
		if s.state.Error != nil {
			return []byte(s.state.ID), nil
		}
		return nil, appErr.ToLambdaError(err)
	}

	return []byte(s.state.ID), nil
}

func (s *Saga) StartLambda() {
	lambda.StartHandler(s)
}

func (s *Saga) recovery(ctx context.Context, err *errors.Error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(s.state)
			s.runErrorHandler(ctx, *err)
			s.state.step.Fail(*err)
			s.state.updateStep()
			s.save(ctx)
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(s.state)
			s.runErrorHandler(ctx, *err)
			s.state.step.Fail(*err)
			s.state.updateStep()
			s.save(ctx)
		}
	}
}

func (s *Saga) runErrorHandler(ctx context.Context, err errors.Error) {
	for _, errhandler := range s.errorhandlers {
		errhandler(ctx, s.state, err)
	}
}

func (s *Saga) CurrentStep() *Step {
	return s.state.step
}

func (s *Saga) CurrentState() *State {
	return s.state
}

func (s *Saga) FastRetry() {
	for i := range s.state.defs.Definitions {
		s.state.defs.Definitions[i].IntervalSeconds = 0
	}
}
