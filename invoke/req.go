package invoke

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/internal/common"
)

const defaultReqID = "zamus"

type SagaRequest struct {
	fn     string
	Input  jsoniter.RawMessage `json:"input"`
	Resume string              `json:"resume"`
}

func NewSagaRequest(fn string) *SagaRequest {
	return &SagaRequest{fn: fn}
}

func (e *SagaRequest) WithInput(input interface{}) *SagaRequest {
	inputByte, err := common.MarshalJSON(input)
	if err != nil {
		panic(err)
	}

	e.Input = inputByte

	return e
}

func (e *SagaRequest) WithResume(id string) *SagaRequest {
	e.Resume = id

	return e
}

func (e *SagaRequest) MarshalRequest() ([]byte, errors.Error) {
	return common.MarshalJSON(e)
}

type Request struct {
	Method     string      `json:"method"`
	Input      interface{} `json:"input,omitempty"`
	Identity   *Identity   `json:"identity,omitempty"`
	Warmer     bool        `json:"warmer,omitempty"`
	Concurency int         `json:"concurency,omitempty"`
}

func NewRequest(method string) *Request {
	return &Request{
		Method: method,
		Identity: &Identity{
			ID: defaultReqID,
		},
	}
}

func (e *Request) WithIdentity(id *Identity) *Request {
	e.Identity = id

	return e
}

func (e *Request) WithPermission(key, val string) *Request {
	if e.Identity == nil {
		e.Identity = &Identity{
			Pems: make(map[string]string),
		}
	}

	if e.Identity.Pems == nil {
		e.Identity.Pems = make(map[string]string)
	}

	e.Identity.Pems[key] = val

	return e
}

func (e *Request) WithInput(input interface{}) *Request {
	e.Input = input

	return e
}

func (e *Request) MarshalRequest() ([]byte, errors.Error) {
	return common.MarshalJSON(e)
}

type Requests []*Request

//noinspection GoUnusedExportedFunction
func NewRequests(size int) Requests {
	return make(Requests, 0, size)
}

func (r Requests) Add(method string, id *Identity, inputs ...interface{}) Requests {
	for _, input := range inputs {
		//noinspection GoAssignmentToReceiver
		r = append(r, NewRequest(method).WithInput(input).WithIdentity(id))
	}

	return r
}

type BatchResults []*BatchResult

type BatchResult struct {
	Error *errors.AppError    `json:"error,omitempty"`
	Data  jsoniter.RawMessage `json:"data,omitempty"`
}

func (r *BatchResult) UnmarshalData(v interface{}) errors.Error {
	if r.Data != nil {
		return common.UnmarshalJSON(r.Data, v)
	}

	return appErr.ErrUnableUnmarshal.WithCaller()
}

type Identity struct {
	ID     string            `json:"id,omitempty"`
	Email  string            `json:"email,omitempty"`
	IPs    []string          `json:"ips,omitempty"`
	Groups []string          `json:"groups,omitempty"`
	Pems   map[string]string `json:"pems,omitempty"`
}
