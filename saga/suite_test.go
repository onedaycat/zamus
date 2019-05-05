package saga

import (
    "context"
    "strconv"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/dlq/storage/memory"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/invoke"
    "github.com/onedaycat/zamus/testdata/domain"
)

type stubSource struct{}

func (s *stubSource) GetRequest(ctx context.Context, payload []byte) ([]*Request, errors.Error) {
    rec := &invoke.SagaRequest{}
    if err := common.UnmarshalJSON(payload, rec); err != nil {
        return nil, err
    }

    reqs := make([]*Request, 0, 1)

    if rec.Resume != "" {
        reqs = append(reqs, &Request{
            Resume: rec.Resume,
        })

        return reqs, nil
    }

    if rec.EventMsg != nil {
        msg := &event.Msg{}
        _ = event.UnmarshalMsg(rec.EventMsg, msg)

        reqs = append(reqs, &Request{
            EventMsg: msg,
        })

        return reqs, nil
    }

    return reqs, nil
}

type testdata struct {
    ID string `json:"id"`
}

func (s *testdata) Inc() {
    i, _ := strconv.Atoi(s.ID)
    i++
    s.ID = strconv.Itoa(i)
}

type testHandler struct {
    spy            *common.SpyTest
    startStep      string
    startError     bool
    parseError     bool
    s1handlerMode  string
    s2handlerMode  string
    s2NextNotFound bool
    s2Panic        bool
    s2PanicString  bool
    s3handlerMode  string
    s1CompMode     string
    s2CompMode     string
    s3CompMode     string
}

func (h *testHandler) StateDefinitions() *StateDefinitions {
    return &StateDefinitions{
        Name:                "Test",
        Event:               event.EventType((*domain.StockItemCreated)(nil)),
        ReturnFailedOnError: false,
        Definitions: []*StateDefinition{
            {
                Name:              "s1",
                Retry:             2,
                IntervalSeconds:   1,
                BackoffRate:       2,
                StepHandler:       h.S1Handler,
                CompensateHandler: h.S1Compensate,
            },
            {
                Name:              "s2",
                Retry:             2,
                IntervalSeconds:   1,
                BackoffRate:       2,
                StepHandler:       h.S2Handler,
                CompensateHandler: h.S2Compensate,
            },
            {
                Name:              "s3",
                Retry:             2,
                IntervalSeconds:   1,
                BackoffRate:       2,
                StepHandler:       h.S3Handler,
                CompensateHandler: h.S3Compensate,
            },
        },
    }
}

func (h *testHandler) Start(ctx context.Context, msg *event.Msg) (string, interface{}, errors.Error) {
    h.spy.Called("start")
    data := &testdata{}
    evt := &domain.StockItemCreated{}
    err := msg.UnmarshalEvent(evt)
    if err != nil {
        return "", nil, err
    }

    data = &testdata{ID: evt.Id}
    if h.startError {
        return h.startStep, data, errors.DumbError
    }

    return h.startStep, data, nil
}

func (h *testHandler) ParseResume(dataPayload Payload) (interface{}, errors.Error) {
    h.spy.Called("resume")
    if h.parseError {
        return nil, errors.DumbError
    }

    data := &testdata{}
    if err := dataPayload.Unmarshal(data); err != nil {
        return nil, err
    }

    return data, nil
}

func (h *testHandler) S1Handler(ctx context.Context, data interface{}, action StepAction) {
    h.spy.Called("s1")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s1handlerMode {
    case "next":
        action.Next("s2", tdata)
    case "error":
        action.Error(errors.DumbError)
    case "fail":
        action.Fail(errors.DumbError)
    case "compensate":
        action.Compensate(errors.DumbError, tdata)
    case "partial_compensate":
        action.PartialCompensate(errors.DumbError, tdata)
    case "partial_error":
        action.PartialError(errors.DumbError)
    case "stop":
        action.Stop(tdata)
    case "end":
        action.End(tdata)
    }
}

func (h *testHandler) S1Compensate(ctx context.Context, data interface{}, action CompensateAction) {
    h.spy.Called("s1comp")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s1CompMode {
    case "error":
        action.Error(errors.DumbError)
    case "fail":
        action.Fail(errors.DumbError)
    case "back":
        action.Back(tdata)
    }
}

func (h *testHandler) S2Handler(ctx context.Context, data interface{}, action StepAction) {
    h.spy.Called("s2")
    tdata := data.(*testdata)
    tdata.Inc()

    if h.s2Panic {
        panic(errors.DumbError)
    }

    if h.s2PanicString {
        panic("panic")
    }

    switch h.s2handlerMode {
    case "next":
        if h.s2NextNotFound {
            action.Next("s00", tdata)
        } else {
            action.Next("s3", tdata)
        }
    case "error":
        action.Error(errors.DumbError)
    case "fail":
        action.Fail(errors.DumbError)
    case "compensate":
        action.Compensate(errors.DumbError, tdata)
    case "partial_compensate":
        action.PartialCompensate(errors.DumbError, tdata)
    case "partial_error":
        action.PartialError(errors.DumbError)
    case "stop":
        action.Stop(tdata)
    case "end":
        action.End(tdata)
    }
}

func (h *testHandler) S2Compensate(ctx context.Context, data interface{}, action CompensateAction) {
    h.spy.Called("s2comp")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s2CompMode {
    case "error":
        action.Error(errors.DumbError)
    case "fail":
        action.Fail(errors.DumbError)
    case "back":
        action.Back(tdata)
    }
}

func (h *testHandler) S3Handler(ctx context.Context, data interface{}, action StepAction) {
    h.spy.Called("s3")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s3handlerMode {
    case "next":
        action.Next("s4", tdata)
    case "error":
        action.Error(errors.DumbError)
    case "fail":
        action.Fail(errors.DumbError)
    case "compensate":
        action.Compensate(errors.DumbError, tdata)
    case "partial_compensate":
        action.PartialCompensate(errors.DumbError, tdata)
    case "partial_error":
        action.PartialError(errors.DumbError)
    case "stop":
        action.Stop(tdata)
    case "end":
        action.End(tdata)
    }
}

func (h *testHandler) S3Compensate(ctx context.Context, data interface{}, action CompensateAction) {
    h.spy.Called("s3comp")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s3CompMode {
    case "error":
        action.Error(errors.DumbError)
    case "fail":
        action.Fail(errors.DumbError)
    case "back":
        action.Back(tdata)
    }
}

type handlerSuite struct {
    saga    *Saga
    handle  *testHandler
    storage dlq.Storage
    ctx     context.Context
    defs    *StateDefinitions
    msg     *event.Msg
    msgByte []byte
    req     *Request
}

func setupHandlerSuite() *handlerSuite {
    h := &handlerSuite{}

    h.handle = &testHandler{
        spy:       common.Spy(),
        startStep: "s1",
    }

    h.storage = memory.New()

    h.defs = h.handle.StateDefinitions()
    h.saga = New(&stubSource{}, &Config{
        Handlers:   []SagaHandle{h.handle},
        FastRetry:  true,
        DLQStorage: h.storage,
    })
    h.ctx = context.Background()
    h.saga.ErrorHandlers(PrintPanic)

    return h
}

func (h *handlerSuite) WithStartError() *handlerSuite {
    h.handle.startError = true
    return h
}

func (h *handlerSuite) WithStartStep(step string) *handlerSuite {
    h.handle.startStep = step
    return h
}

func (h *handlerSuite) WithS1Handler(mode string) *handlerSuite {
    h.handle.s1handlerMode = mode
    return h
}

func (h *handlerSuite) WithS2Handler(mode string) *handlerSuite {
    h.handle.s2handlerMode = mode
    return h
}

func (h *handlerSuite) WithS2NextStepNotfound() *handlerSuite {
    h.handle.s2NextNotFound = true
    return h
}

func (h *handlerSuite) WithS3Handler(mode string) *handlerSuite {
    h.handle.s3handlerMode = mode
    return h
}

func (h *handlerSuite) WithS1Compensate(mode string) *handlerSuite {
    h.handle.s1CompMode = mode
    return h
}

func (h *handlerSuite) WithS2Compensate(mode string) *handlerSuite {
    h.handle.s2CompMode = mode
    return h
}

func (h *handlerSuite) WithS3Compensate(mode string) *handlerSuite {
    h.handle.s3CompMode = mode
    return h
}

func (h *handlerSuite) WithS2Panic() *handlerSuite {
    h.handle.s2Panic = true
    return h
}

func (h *handlerSuite) WithS2PanicString() *handlerSuite {
    h.handle.s2PanicString = true
    return h
}

func (h *handlerSuite) WithGetResumeParseError() *handlerSuite {
    h.handle.parseError = true
    return h
}

func (h *handlerSuite) WithReutnFailedOnError() *handlerSuite {
    for _, state := range h.saga.states {
        state.defs.ReturnFailedOnError = true
    }

    return h
}

func (h *handlerSuite) WithEventMsg(id string) *handlerSuite {
    evt := &domain.StockItemCreated{Id: id}
    evtByte, _ := event.MarshalEvent(evt)
    h.msg = &event.Msg{Id: "e1", AggID: id, Event: evtByte, EventType: event.EventType((*domain.StockItemCreated)(nil))}
    h.msgByte, _ = invoke.NewSagaRequest("fn").WithEventMsg(h.msg).MarshalRequest()
    h.req = &Request{EventMsg: h.msg}

    return h
}
