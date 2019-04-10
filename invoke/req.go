package invoke

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
)

const defaultReqID = "zamus"

type SagaRequest struct {
	Input  jsoniter.RawMessage `json:"input"`
	Resume string              `json:"resume"`
}

func NewSagaRequest() *SagaRequest {
	return &SagaRequest{}
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
	Function   string      `json:"function"`
	Args       interface{} `json:"arguments,omitempty"`
	Identity   *Identity   `json:"identity,omitempty"`
	Warmer     bool        `json:"warmer,omitempty"`
	Concurency int         `json:"concurency,omitempty"`
	index      int
}

func NewRequest(fn string) *Request {
	return &Request{
		Function: fn,
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

func (e *Request) WithArgs(args interface{}) *Request {
	e.Args = args

	return e
}

func (e *Request) MarshalRequest() ([]byte, errors.Error) {
	return common.MarshalJSON(e)
}

type Requests []*Request

func NewRequests(size int) Requests {
	return make(Requests, 0, size)
}

func (r Requests) Add(fn string, id *Identity, argsList ...interface{}) Requests {
	for _, args := range argsList {
		r = append(r, NewRequest(fn).WithArgs(args).WithIdentity(id))
	}

	return r
}

type BatchResults []*BatchResult

type BatchResult struct {
	Error *errors.AppError `json:"error,omitempty"`
	Data  interface{}      `json:"data,omitempty"`
}

type Identity struct {
	ID     string            `json:"id,omitempty"`
	Email  string            `json:"email,omitempty"`
	IPs    []string          `json:"ips,omitempty"`
	Groups []string          `json:"groups,omitempty"`
	Pems   map[string]string `json:"pems,omitempty"`
}
