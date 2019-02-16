package invoke

import (
	"context"
	"encoding/json"
)

type InvokePreHandler func(ctx context.Context, event *InvokeEvent) error
type InvokePostHandler func(ctx context.Context, event *InvokeEvent, result *Result) error
type InvokeEventHandler func(ctx context.Context, event *InvokeEvent) *Result
type InvokeErrorHandler func(ctx context.Context, event *InvokeEvent, err error)

type invokeHandlers struct {
	handler      InvokeEventHandler
	preHandlers  []InvokePreHandler
	postHandlers []InvokePostHandler
}

type InvokeEvents []*InvokeEvent

type InvokeEvent struct {
	Function      string          `json:"function"`
	Args          json.RawMessage `json:"arguments"`
	Source        json.RawMessage `json:"source"`
	Identity      *Identity       `json:"identity"`
	PermissionKey string          `json:"pemKey"`
}

func (e *InvokeEvent) ParseArgs(v interface{}) error {
	return json.Unmarshal(e.Args, v)
}

func (e *InvokeEvent) ParseSource(v interface{}) error {
	return json.Unmarshal(e.Source, v)
}

func (e *InvokeEvent) Result(data interface{}) *Result {
	return &Result{
		Data:  data,
		Error: nil,
	}
}

func (e *InvokeEvent) ErrorResult(err error) *Result {
	return &Result{
		Data:  nil,
		Error: makeError(err),
	}
}

type Result struct {
	Data  interface{} `json:"data"`
	Error error       `json:"error"`
}
