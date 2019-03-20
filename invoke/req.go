package invoke

import (
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
)

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
