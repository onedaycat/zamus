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
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/zamuscontext"
)

const (
	emptyStr = ""
)

type SagaHandle interface {
	StateDefinitions() *StateDefinitions
	Start(ctx context.Context, input Payload) (string, interface{}, errors.Error)
	ParseData(dataPayload Payload) (interface{}, errors.Error)
}

type Handler func(ctx context.Context, data interface{}, stepAction StepAction)
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

	if req.Input != nil {
		if err := s.doStart(zmctx); err != nil {
			return err
		}
	} else if req.Resume != emptyStr {
		if err := s.doResume(zmctx); err != nil {
			return err
		}
	} else {
		return nil
	}

	if len(s.state.defs.Definitions) == 0 || len(s.state.Steps) == 0 {
		return nil
	}

	for {
		if s.state.Action == END {
			break
		}

		s.doHandler(zmctx)
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
	s.state.ID = eid.GenerateID()
	s.state.StartTime = clock.Now().Unix()
	s.state.Input = s.req.Input
	stateName, data, err = s.handle.Start(ctx, Payload(s.state.Input))
	if err != nil {
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, err)
		}
		return err
	}

	err = s.state.newStep(stateName, data)
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

	s.state.handler(ctx, s.state.data, s.state.step)

	if s.state.step.Status == WAIT {
		s.state.step.End(s.state.step.data)
	}

	s.state.updateStep()

	switch s.state.Status {
	case SUCCESS:
		switch s.state.Action {
		case NEXT:
			if s.state.Compensate {
				err = appErr.ErrNextOnCompensateNotAllowed.WithCaller().WithInput(s.state)
				s.state.step.Fail(err)
				s.state.updateStep()
				for _, errhandler := range s.errorhandlers {
					errhandler(ctx, s.state, err)
				}

				s.save(ctx)
				return
			}

			if err = s.state.nextStep(); err != nil {
				s.state.step.Fail(err)
				s.state.updateStep()
				for _, errhandler := range s.errorhandlers {
					errhandler(ctx, s.state, err)
				}

				s.save(ctx)
				return
			}
		case END:
			return
		}
	case ERROR:
		if s.state.step.retry() {
			time.Sleep(s.state.step.sleepDuration())
			return
		}
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, appErr.ErrRetryExceed.WithCaller().WithInput(s.state))
		}
		fallthrough
	case COMPENSATE:
		s.state.backStep()
	case FAILED:
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, s.state.Error)
		}
		s.save(ctx)
	}
}

func (s *Saga) save(ctx context.Context) {
	if err := s.storage.Save(ctx, s.state); err != nil {
		for _, errhandler := range s.errorhandlers {
			errhandler(ctx, s.state, err)
		}
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
			for _, errhandler := range s.errorhandlers {
				errhandler(ctx, s.state, *err)
			}
			s.state.step.Fail(*err)
			s.state.updateStep()
			s.save(ctx)
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(s.state)
			for _, errhandler := range s.errorhandlers {
				errhandler(ctx, s.state, *err)
			}
			s.state.step.Fail(*err)
			s.state.updateStep()
			s.save(ctx)
		}
	}
}
