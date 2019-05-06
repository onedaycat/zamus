package sagacmd

import (
    "context"
    "strconv"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/dlq/storage/memory"
    "github.com/onedaycat/zamus/internal/common"
)

type testdata struct {
    ID string `json:"id"`
}

func (s *testdata) Reset() {
    s.ID = "1"
}

func (s *testdata) Inc() {
    i, _ := strconv.Atoi(s.ID)
    i++
    s.ID = strconv.Itoa(i)
}

type testHandler struct {
    spy            *common.SpyTest
    startStep      string
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
        Definitions: []*StateDefinition{
            {
                Name:              "s1",
                StepHandler:       h.S1Handler,
                CompensateHandler: h.S1Compensate,
            },
            {
                Name:              "s2",
                StepHandler:       h.S2Handler,
                CompensateHandler: h.S2Compensate,
            },
            {
                Name:              "s3",
                StepHandler:       h.S3Handler,
                CompensateHandler: h.S3Compensate,
            },
        },
    }
}

func (h *testHandler) S1Handler(ctx context.Context, data interface{}, action StepAction) {
    h.spy.Called("s1")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s1handlerMode {
    case "next":
        action.Next("s2", tdata)
    case "fail":
        action.Fail(errors.DumbError)
    case "compensate":
        action.Compensate(errors.DumbError, tdata)
    case "partial_compensate":
        action.PartialCompensate(errors.DumbError, tdata)
    case "end":
        action.End(tdata)
    }
}

func (h *testHandler) S1Compensate(ctx context.Context, data interface{}, action CompensateAction) {
    h.spy.Called("s1comp")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s1CompMode {
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
    case "fail":
        action.Fail(errors.DumbError)
    case "compensate":
        action.Compensate(errors.DumbError, tdata)
    case "partial_compensate":
        action.PartialCompensate(errors.DumbError, tdata)
    case "end":
        action.End(tdata)
    }
}

func (h *testHandler) S2Compensate(ctx context.Context, data interface{}, action CompensateAction) {
    h.spy.Called("s2comp")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s2CompMode {
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
    case "fail":
        action.Fail(errors.DumbError)
    case "compensate":
        action.Compensate(errors.DumbError, tdata)
    case "partial_compensate":
        action.PartialCompensate(errors.DumbError, tdata)
    case "end":
        action.End(tdata)
    }
}

func (h *testHandler) S3Compensate(ctx context.Context, data interface{}, action CompensateAction) {
    h.spy.Called("s3comp")
    tdata := data.(*testdata)
    tdata.Inc()
    switch h.s3CompMode {
    case "fail":
        action.Fail(errors.DumbError)
    case "back":
        action.Back(tdata)
    }
}

type handlerSuite struct {
    saga    *SagaCmd
    handle  *testHandler
    storage dlq.Storage
    ctx     context.Context
    defs    *StateDefinitions
    data    *testdata
}

func setupHandlerSuite() *handlerSuite {
    h := &handlerSuite{}

    h.handle = &testHandler{
        spy:       common.Spy(),
        startStep: "s1",
    }

    h.storage = memory.New()

    h.defs = h.handle.StateDefinitions()
    h.saga = New(h.handle)
    h.ctx = context.Background()

    return h
}

func (h *handlerSuite) WithData() *handlerSuite {
    h.data = &testdata{ID: "1"}
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
