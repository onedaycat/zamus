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
    "github.com/onedaycat/zamus/dlq"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/internal/common/clock"
    "github.com/onedaycat/zamus/internal/common/eid"
    "github.com/onedaycat/zamus/tracer"
    "github.com/onedaycat/zamus/zamuscontext"
)

const (
    emptyStr = ""
)

//noinspection GoNameStartsWithPackageName
type SagaHandle interface {
    // StateDefinitions is a definition of handlers
    // If do not define these, Saga won't work.
    StateDefinitions() *StateDefinitions
    // Start will be run when the request payload has input field.
    // and return the start state name which defined from StateDefinitions() with parsed data to be yours format.
    // If error, it will response to the requester
    Start(ctx context.Context, msg *event.Msg) (string, interface{}, errors.Error)
    // ParseData will be run when the request payload has resume field.
    // Saga load data from the storage and must to parse back to yours data format
    ParseResume(dataPayload Payload) (interface{}, errors.Error)
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
    SentryDSN     string
    Handlers      []SagaHandle
    FastRetry     bool
    DLQStorage    dlq.Storage
}

type Saga struct {
    errorhandlers []ErrorHandler
    zcctx         *zamuscontext.ZamusContext
    handles       map[string]SagaHandle
    handle        SagaHandle
    req           *Request
    states        map[string]*State
    state         *State
    source        Source
    dlqStorage    dlq.Storage
    config        *Config
}

func New(source Source, config *Config) *Saga {
    s := &Saga{
        source:     source,
        states:     make(map[string]*State),
        handles:    make(map[string]SagaHandle),
        req:        &Request{},
        dlqStorage: config.DLQStorage,
        config:     config,
    }

    for _, handler := range config.Handlers {
        state := newState()
        state.defs = handler.StateDefinitions()
        state.Name = state.defs.Name
        state.Fn = lambdacontext.FunctionName

        if config.FastRetry {
            for i := range state.defs.Definitions {
                state.defs.Definitions[i].IntervalSeconds = 0
            }
        }

        s.states[state.defs.Event] = state
        s.handles[state.defs.Event] = handler
    }

    if tracer.Enable {
        s.ErrorHandlers(TraceError)
    }

    if config.SentryDSN != "" {
        sentry.SetDSN(config.SentryDSN)
        sentry.SetOptions(
            sentry.WithEnv(config.AppStage),
            sentry.WithServerName(lambdacontext.FunctionName),
            sentry.WithServiceName(config.Service),
            sentry.WithRelease(config.Service+"@"+config.Version),
            sentry.WithVersion(config.Version),
            sentry.WithTags(sentry.Tags{
                {Key: "lambdaVersion", Value: lambdacontext.FunctionVersion},
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

    if req.EventMsg != nil {
        state, ok := s.states[req.EventMsg.EventType]
        if !ok {
            return nil
        }
        s.handle = s.handles[req.EventMsg.EventType]
        s.state = state
        s.state.Clear()
        s.req = req

        if err := s.doStart(zmctx); err != nil {
            return err
        }
    } else if req.Resume != emptyStr {
        s.req = req
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
            if s.state.Status == WAIT {
                break
            }
        }
    }

    s.state.Data, _ = common.MarshalJSON(s.state.data)

    if s.state.Error != nil {
        return s.state.Error
    }

    return nil
}

func (s *Saga) doResume(ctx context.Context) (err errors.Error) {
    defer s.recovery(ctx, &err)
    s.state = newState()
    dlqMsg, err := s.dlqStorage.Get(ctx, dlq.Saga, s.req.Resume)
    if err != nil {
        for _, errhandler := range s.errorhandlers {
            errhandler(ctx, s.state, err)
        }

        return err
    }
    s.state, err = ParseStateFromDLQMsg(dlqMsg)
    if err != nil {
        for _, errhandler := range s.errorhandlers {
            errhandler(ctx, s.state, err)
        }

        return err
    }

    var data interface{}
    s.handle = s.handles[s.state.EventMsg.EventType]
    data, err = s.handle.ParseResume(Payload(s.state.Data))
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

    stateName, data, err = s.handle.Start(ctx, s.req.EventMsg)
    if err != nil {
        for _, errhandler := range s.errorhandlers {
            errhandler(ctx, s.state, err)
        }
        return err
    }

    s.state.setupStart(s.req.EventMsg)
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
    case WAIT:
        s.save(ctx, s.state)
        break
    case ERROR:
        switch s.state.Action {
        case RETRY:
            if s.state.step.retry() {
                time.Sleep(s.state.step.sleepDuration())
                break
            }
            if s.state.step.errPartial {
                s.state.index++
                s.state.step.PartialCompensate(s.state.step.StepError, s.state.data)
                s.state.updateStep()
            } else {
                s.state.step.Compensate(s.state.step.StepError, s.state.data)
                s.state.updateStep()
            }
            s.runErrorHandler(ctx, s.state.step.StepError)
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
        s.save(ctx, s.state)
    }
}

func (s *Saga) doCompensate(ctx context.Context) {
    var err errors.Error
    defer s.recovery(ctx, &err)

    s.state.step.def.CompensateHandler(ctx, s.state.data, s.state.step)

    if s.state.step.Status == INIT {
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
        s.state.step.Fail(s.state.step.StepError)
        s.state.updateStep()
        s.runErrorHandler(ctx, s.state.Error)
    }

    if s.state.Status == FAILED {
        s.runErrorHandler(ctx, s.state.Error)
        s.save(ctx, s.state)
    }
}

func (s *Saga) save(ctx context.Context, state *State) {
    state.Data, _ = common.MarshalJSON(state.data)
    stateByte, _ := common.MarshalJSON(state)
    if s.dlqStorage != nil {
        dlqMsg := &dlq.DLQMsg{
            ID:         eid.GenerateID(),
            Service:    s.config.Service,
            Time:       clock.Now().Unix(),
            Version:    s.config.Version,
            Data:       stateByte,
            Fn:         lambdacontext.FunctionName,
            LambdaType: dlq.Saga,
        }

        if state.Error != nil {
            var stacks []string
            causeMsg := emptyStr
            cause := state.Error.GetCause()
            if cause != nil {
                causeMsg = cause.Error()
            }

            if state.Error.HasStackTrace() {
                stacks = state.Error.StackStrings()
            }

            dlqMsg.Errors = dlq.DLQErrors{
                {
                    Message: state.Error.GetMessage(),
                    Cause:   causeMsg,
                    Input:   s.req,
                    Stacks:  stacks,
                },
            }
        }

        err := s.dlqStorage.Save(ctx, dlqMsg)
        if err != nil {
            s.runErrorHandler(ctx, err)
        }
    }
}

func (s *Saga) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
    reqs, err := s.source.GetRequest(ctx, payload)
    if err != nil {
        return nil, appErr.ToLambdaError(appErr.ErrInvalidRequest.WithCause(err).WithCaller().WithInput(string(payload)))
    }

    for _, req := range reqs {
        s.req.clear()
        if err = s.Handle(ctx, req); err != nil {
            if s.state != nil && s.state.Error != nil {
                if s.state.defs.ReturnFailedOnError {
                    return nil, appErr.ToLambdaError(s.state.Error)
                }
                return []byte("success"), nil
            }
            return nil, appErr.ToLambdaError(err)
        }
    }

    return []byte("success"), nil
}

func (s *Saga) Handler(ctx context.Context, payload jsoniter.RawMessage) (interface{}, error) {
    reqs, err := s.source.GetRequest(ctx, payload)
    if err != nil {
        return nil, appErr.ToLambdaError(appErr.ErrInvalidRequest.WithCause(err).WithCaller().WithInput(string(payload)))
    }

    for _, req := range reqs {
        s.req.clear()
        if err = s.Handle(ctx, req); err != nil {
            if s.state != nil && s.state.Error != nil {
                if s.state.defs.ReturnFailedOnError {
                    return nil, appErr.ToLambdaError(s.state.Error)
                }
                return "success", nil
            }
            return nil, appErr.ToLambdaError(err)
        }
    }

    return "success", nil
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
            s.save(ctx, s.state)
        default:
            *err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(s.state)
            s.runErrorHandler(ctx, *err)
            s.state.step.Fail(*err)
            s.state.updateStep()
            s.save(ctx, s.state)
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

func ParseStateFromDLQMsg(dqlMsg *dlq.DLQMsg) (*State, errors.Error) {
    state := newState()
    err := common.UnmarshalJSON(dqlMsg.Data, state)
    if err != nil {
        return nil, err
    }

    if state.Error != nil {
        state.Error = appErr.ErrorByCode(state.Error).(*errors.AppError)
    }

    return state, nil
}
