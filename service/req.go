package service

import (
    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common"
)

const (
    firstCharArray  = 91
    firstCharObject = 123
    emptyStr        = ""
)

type Request struct {
    Method     string              `json:"method"`
    Input      jsoniter.RawMessage `json:"arguments,omitempty"`
    Identity   *Identity           `json:"identity,omitempty"`
    Warmer     bool                `json:"warmer,omitempty"`
    Concurency int                 `json:"concurency,omitempty"`
    index      int
}

func NewRequest(fn string) *Request {
    return &Request{
        Method: fn,
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

func (e *Request) WithInput(input interface{}) *Request {
    var err error
    e.Input, err = common.MarshalJSON(input)
    if err != nil {
        panic(appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(input))
    }

    return e
}

func (e *Request) ParseInput(v interface{}) errors.Error {
    if len(e.Input) == 0 {
        return nil
    }

    if err := common.UnmarshalJSON(e.Input, v); err != nil {
        return err
    }

    return nil
}

func (e *Request) MarshalRequest() ([]byte, errors.Error) {
    return common.MarshalJSON(e)
}

func (e *Request) Clear() {
    e.Method = emptyStr
    e.Input = nil
    e.Identity = nil
    e.Warmer = false
    e.Concurency = 0
    e.index = 0
}

type Requests []*Request

//noinspection GoUnusedExportedFunction
func NewRequests(size int) Requests {
    return make(Requests, 0, size)
}

func (r Requests) Add(fn string, id *Identity, inputs ...interface{}) Requests {
    for _, input := range inputs {
        //noinspection GoAssignmentToReceiver
        r = append(r, NewRequest(fn).WithInput(input).WithIdentity(id))
    }

    return r
}

type BatchResults []*BatchResult

type MapBatchResults interface {
    GetID(index int) string
    Get(index int) interface{}
    Len() int
    Missed() errors.Error
}

func (r BatchResults) MarshalResponses() ([]byte, errors.Error) {
    return common.MarshalJSON(r)
}

func (r BatchResults) Map(data MapBatchResults, ids []string) {
    for i := 0; i < data.Len(); i++ {
        for j := range ids {
            if data.GetID(i) == ids[j] {
                r[j].Data = data.Get(i)
            }
        }
    }

    for i := range r {
        if r[i].Data == nil {
            r[i].Error = data.Missed().(*errors.AppError)
        }
    }
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
        for i := range q.reqs {
            q.reqs[i].Clear()
        }

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
        q.req.Clear()

        if err = common.UnmarshalJSON(b, q.req); err != nil {
            return err
        }

        q.totalIndex = 0

        return nil
    }

    return appErr.ErrUnableParseRequest.WithInput(string(b)).WithCaller()
}
