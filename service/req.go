package service

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
)

const (
	firstCharArray  = 91
	firstCharObject = 123
)

type Request struct {
	Function   string              `json:"function"`
	Args       jsoniter.RawMessage `json:"arguments,omitempty"`
	Identity   *Identity           `json:"identity,omitempty"`
	Warmer     bool                `json:"warmer,omitempty"`
	Concurency int                 `json:"concurency,omitempty"`
	index      int
}

func NewRequest(fn string) *Request {
	return &Request{
		Function: fn,
	}
}

func (e *Request) WithIdentity(id *Identity) *Request {
	e.Identity = id

	return e
}

func (e *Request) WithPermission(key, val string) *Request {
	if e.Identity == nil {
		e.Identity = &Identity{}
	}
	e.Identity.Pems[key] = val

	return e
}

func (e *Request) WithArgs(args interface{}) *Request {
	var err error
	e.Args, err = common.MarshalJSON(args)
	if err != nil {
		panic(appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(args))
	}

	return e
}

func (e *Request) ParseArgs(v interface{}) errors.Error {
	if err := common.UnmarshalJSON(e.Args, v); err != nil {
		return err
	}

	return nil
}

func (e *Request) MarshalRequest() ([]byte, errors.Error) {
	return common.MarshalJSON(e)
}

type Requests []*Request

func NewRequests(size int) Requests {
	return make(Requests, size)
}

func (r Requests) Add(fn string, id *Identity, argsList ...interface{}) Requests {
	for _, args := range argsList {
		r = append(r, NewRequest(fn).WithArgs(args).WithIdentity(id))
	}

	return r
}

type BatchResults []*BatchResult

func (r BatchResults) MarshalResponses() ([]byte, errors.Error) {
	return common.MarshalJSON(r)
}

type BatchResult struct {
	Error *errors.AppError `json:"error,omitempty"`
	Data  interface{}      `json:"data,omitempty"`
}

func (r *BatchResult) MarshalResponse() ([]byte, errors.Error) {
	return common.MarshalJSON(r)
}

type mainReq struct {
	req         *Request
	reqs        []*Request
	batchResult BatchResults
	totalIndex  int
}

func (q *mainReq) UnmarshalRequest(b []byte) error {
	var err error
	firstChar := b[0]

	if firstChar == firstCharArray {
		if err = common.UnmarshalJSON(b, &q.reqs); err != nil {
			return err
		}

		q.totalIndex = len(q.reqs)
		if q.totalIndex > 10 {
			return appErr.ErrBatchRequestExceed.WithCaller().WithInput(string(b))
		}

		q.batchResult = q.batchResult[:q.totalIndex]
		for i, req := range q.reqs {
			req.index = i
			q.batchResult[i].Error = nil
			q.batchResult[i].Data = nil
		}

		return nil
	} else if firstChar == firstCharObject {
		if err = common.UnmarshalJSON(b, q.req); err != nil {
			return err
		}

		q.totalIndex = 0

		return nil
	}

	return appErr.ErrUnableParseRequest.WithInput(string(b)).WithCaller()
}
