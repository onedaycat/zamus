package invoke

import (
	"context"
	"encoding/json"
)

type ResultInterator func(index int) *Result

type BatchInvokePreHandler func(ctx context.Context, event *BatchInvokeEvent) error
type BatchInvokePostHandler func(ctx context.Context, event *BatchInvokeEvent, results *Results) error
type BatchInvokeEventHandler func(ctx context.Context, event *BatchInvokeEvent) *Results
type BatchInvokeErrorHandler func(ctx context.Context, event *BatchInvokeEvent, err error)

type batchInvokeHandlers struct {
	handler      BatchInvokeEventHandler
	preHandlers  []BatchInvokePreHandler
	postHandlers []BatchInvokePostHandler
}

type BatchInvokeEvent struct {
	Field    string          `json:"field"`
	Args     json.RawMessage `json:"arguments"`
	Sources  json.RawMessage `json:"sources"`
	Identity *Identity       `json:"identity"`
	NSource  int             `json:"-"`
}

func (e *BatchInvokeEvent) ParseArgs(v interface{}) error {
	return json.Unmarshal(e.Args, v)
}

func (e *BatchInvokeEvent) ParseSources(v interface{}) error {
	return json.Unmarshal(e.Sources, v)
}

func (e *BatchInvokeEvent) Result(results []*Result) *Results {
	if len(results) != e.NSource {
		return makeErrorResults(e.NSource, ErrBatchInvokeResultSizeNotMatch)
	}

	result := &Results{
		Results: results,
		Error:   nil,
	}

	return result
}

func (e *BatchInvokeEvent) ErrorResult(err error) *Results {
	return makeErrorResults(e.NSource, err)
}

type Results struct {
	Results []*Result
	Error   error
}

func makeErrorResults(n int, err error) *Results {
	aerr := makeError(err)
	result := &Results{
		Results: make([]*Result, n),
		Error:   aerr,
	}

	for i := 0; i < n; i++ {
		result.Results[i] = &Result{
			Data:  nil,
			Error: aerr,
		}
	}

	return result
}
