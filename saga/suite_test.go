package saga

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
)

type stubStorage struct {
	data      map[string]*State
	getError  bool
	saveError bool
}

func (s *stubStorage) Clear() {
	s.data = make(map[string]*State)
}

func (s *stubStorage) Get(ctx context.Context, id string) (*State, errors.Error) {
	if s.getError {
		return nil, errors.DumbError
	}

	state, ok := s.data[id]
	if !ok {
		return nil, appErr.ErrStateNotFound
	}

	return state, nil
}

func (s *stubStorage) Save(ctx context.Context, state *State) errors.Error {
	if s.saveError {
		return errors.DumbError
	}

	s.data[state.ID] = state

	return nil
}

type testdata struct {
	ID int `json:"id"`
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
		Name: "Test",
		Definitions: []*StateDefinition{
			{
				Name:            "s1",
				Retry:           2,
				IntervalSeconds: 1,
				BackoffRate:     2,
				Handler:         h.S1Handler,
				Compensate:      h.S1Compensate,
			},
			{
				Name:            "s2",
				Retry:           2,
				IntervalSeconds: 1,
				BackoffRate:     2,
				Handler:         h.S2Handler,
				Compensate:      h.S2Compensate,
			},
			{
				Name:            "s3",
				Retry:           2,
				IntervalSeconds: 1,
				BackoffRate:     2,
				Handler:         h.S3Handler,
				Compensate:      h.S3Compensate,
			},
		},
	}
}

func (h *testHandler) Start(ctx context.Context, input Payload) (string, interface{}, errors.Error) {
	h.spy.Called("start")
	data := &testdata{}
	input.Unmarshal(data)

	if h.startError {
		return h.startStep, data, errors.DumbError
	}

	return h.startStep, data, nil
}

func (h *testHandler) ParseData(dataPayload Payload) (interface{}, errors.Error) {
	h.spy.Called("resume")
	if h.parseError {
		return nil, errors.DumbError
	}

	data := &testdata{}
	dataPayload.Unmarshal(data)

	return data, nil
}

func (h *testHandler) S1Handler(ctx context.Context, data interface{}, action HandlerAction) {
	h.spy.Called("s1")
	tdata := data.(*testdata)
	tdata.ID++
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
	case "end":
		action.End(tdata)
	}
}

func (h *testHandler) S1Compensate(ctx context.Context, data interface{}, action CompensateAction) {
	h.spy.Called("s1comp")
	tdata := data.(*testdata)
	tdata.ID++
	switch h.s1CompMode {
	case "error":
		action.Error(errors.DumbError)
	case "fail":
		action.Fail(errors.DumbError)
	case "back":
		action.Back(tdata)
	}
}

func (h *testHandler) S2Handler(ctx context.Context, data interface{}, action HandlerAction) {
	h.spy.Called("s2")
	tdata := data.(*testdata)
	tdata.ID++

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
	case "end":
		action.End(tdata)
	}
}

func (h *testHandler) S2Compensate(ctx context.Context, data interface{}, action CompensateAction) {
	h.spy.Called("s2comp")
	tdata := data.(*testdata)
	tdata.ID++
	switch h.s2CompMode {
	case "error":
		action.Error(errors.DumbError)
	case "fail":
		action.Fail(errors.DumbError)
	case "back":
		action.Back(tdata)
	}
}

func (h *testHandler) S3Handler(ctx context.Context, data interface{}, action HandlerAction) {
	h.spy.Called("s3")
	tdata := data.(*testdata)
	tdata.ID++
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
	case "end":
		action.End(tdata)
	}
}

func (h *testHandler) S3Compensate(ctx context.Context, data interface{}, action CompensateAction) {
	h.spy.Called("s3comp")
	tdata := data.(*testdata)
	tdata.ID++
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
	storage *stubStorage
	ctx     context.Context
	defs    *StateDefinitions
}

func setupHandlerSuite() *handlerSuite {
	h := &handlerSuite{}

	h.handle = &testHandler{
		spy:       common.Spy(),
		startStep: "s1",
	}

	h.storage = &stubStorage{
		data: make(map[string]*State),
	}

	h.defs = h.handle.StateDefinitions()
	h.saga = New(h.handle, h.storage, &Config{})
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

func (h *handlerSuite) WithGetResumeError() *handlerSuite {
	h.storage.getError = true
	return h
}

func (h *handlerSuite) WithGetResumeParseError() *handlerSuite {
	h.handle.parseError = true
	return h
}
