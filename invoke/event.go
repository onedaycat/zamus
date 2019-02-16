package invoke

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/buger/jsonparser"
	"github.com/onedaycat/errors"
)

const (
	eventInvokeType      int = 0
	eventBatchInvokeType int = 1
)

type Request struct {
	InvokeEvent      *InvokeEvent
	BatchInvokeEvent *BatchInvokeEvent
	invokeEvents     []*InvokeEvent
	eventType        int
	sources          json.RawMessage
}

func (r *Request) UnmarshalJSON(b []byte) error {
	_, dataTypeRoot, _, err := jsonparser.Get(b)
	if err != nil {
		return err
	}

	if dataTypeRoot == jsonparser.Array {
		r.invokeEvents = make(InvokeEvents, 0, 5)
		r.eventType = eventBatchInvokeType
		if err = json.Unmarshal(b, &r.invokeEvents); err != nil {
			return err
		}

		if len(r.invokeEvents) == 0 {
			return ErrNoBatchInvokeData
		}

		b := bytes.NewBuffer(nil)
		b.WriteByte(91)
		first := true
		n := 0
		for i := 0; i < len(r.invokeEvents); i++ {
			if len(r.invokeEvents[i].Source) == 0 {
				continue
			}

			if !first {
				b.WriteByte(44)
			}
			b.Write(r.invokeEvents[i].Source)
			first = false
			n = n + 1
		}
		b.WriteByte(93)

		r.BatchInvokeEvent = &BatchInvokeEvent{
			Field:    r.invokeEvents[0].Function,
			Args:     r.invokeEvents[0].Args,
			Sources:  b.Bytes(),
			Identity: r.invokeEvents[0].Identity,
			NSource:  n,
		}

		if len(r.BatchInvokeEvent.Sources) == 2 {
			r.BatchInvokeEvent.Sources = nil
		}

		return nil
	} else if dataTypeRoot == jsonparser.Object {
		r.InvokeEvent = &InvokeEvent{}
		r.eventType = eventInvokeType
		return json.Unmarshal(b, r.InvokeEvent)
	}

	return errors.Newf("Unable to UnmarshalJSON of %s", dataTypeRoot.String())
}

type EventManager struct {
	invokeFields            map[string]*invokeHandlers
	invokeErrorHandler      InvokeErrorHandler
	invokePreHandlers       []InvokePreHandler
	invokePostHandlers      []InvokePostHandler
	batchInvokeFields       map[string]*batchInvokeHandlers
	batchInvokeErrorHandler BatchInvokeErrorHandler
	batchInvokePreHandlers  []BatchInvokePreHandler
	batchInvokePostHandlers []BatchInvokePostHandler
}

func NewEventManager() *EventManager {
	return &EventManager{
		invokeFields:            make(map[string]*invokeHandlers),
		invokeErrorHandler:      func(ctx context.Context, event *InvokeEvent, err error) {},
		invokePreHandlers:       []InvokePreHandler{},
		invokePostHandlers:      []InvokePostHandler{},
		batchInvokeFields:       make(map[string]*batchInvokeHandlers),
		batchInvokeErrorHandler: func(ctx context.Context, event *BatchInvokeEvent, err error) {},
		batchInvokePreHandlers:  []BatchInvokePreHandler{},
		batchInvokePostHandlers: []BatchInvokePostHandler{},
	}
}

func (e *EventManager) OnInvokeError(handler InvokeErrorHandler) {
	e.invokeErrorHandler = handler
}

func (e *EventManager) OnBatchInvokeError(handler BatchInvokeErrorHandler) {
	e.batchInvokeErrorHandler = handler
}

func (e *EventManager) RegisterInvoke(field string, handler InvokeEventHandler, preHandler []InvokePreHandler, postHandler []InvokePostHandler) {
	e.invokeFields[field] = &invokeHandlers{
		handler:      handler,
		preHandlers:  preHandler,
		postHandlers: postHandler,
	}
}

func (e *EventManager) RegisterBatchInvoke(field string, handler BatchInvokeEventHandler, preHandler []BatchInvokePreHandler, postHandler []BatchInvokePostHandler) {
	e.batchInvokeFields[field] = &batchInvokeHandlers{
		handler:      handler,
		preHandlers:  preHandler,
		postHandlers: postHandler,
	}
}

func (e *EventManager) UseInvokePreHandler(handlers ...InvokePreHandler) {
	if len(handlers) == 0 {
		return
	}

	e.invokePreHandlers = handlers
}

func (e *EventManager) UseBatchInvokePreHandler(handlers ...BatchInvokePreHandler) {
	if len(handlers) == 0 {
		return
	}

	e.batchInvokePreHandlers = handlers
}

func (e *EventManager) UseInvokePostHandler(handlers ...InvokePostHandler) {
	if len(handlers) == 0 {
		return
	}

	e.invokePostHandlers = handlers
}

func (e *EventManager) UseBatchInvokePostHandler(handlers ...BatchInvokePostHandler) {
	if len(handlers) == 0 {
		return
	}

	e.batchInvokePostHandlers = handlers
}

func (e *EventManager) runInvokePreHandler(ctx context.Context, event *InvokeEvent, handlers []InvokePreHandler) *Result {
	for _, handler := range handlers {
		if err := handler(ctx, event); err != nil {
			e.invokeErrorHandler(ctx, event, err)
			return &Result{
				Data:  nil,
				Error: makeError(err),
			}
		}
	}

	return nil
}

func (e *EventManager) runInvokePostHandler(ctx context.Context, event *InvokeEvent, result *Result, handlers []InvokePostHandler) *Result {
	for _, handler := range handlers {
		if err := handler(ctx, event, result); err != nil {
			e.invokeErrorHandler(ctx, event, err)
			return &Result{
				Data:  nil,
				Error: makeError(err),
			}
		}
	}

	return nil
}

func (e *EventManager) runBatchInvokePreHandler(ctx context.Context, event *BatchInvokeEvent, handlers []BatchInvokePreHandler) *Results {
	for _, handler := range handlers {
		if err := handler(ctx, event); err != nil {
			e.batchInvokeErrorHandler(ctx, event, err)
			return makeErrorResults(event.NSource, err)
		}
	}

	return nil
}

func (e *EventManager) runBatchInvokePostHandler(ctx context.Context, event *BatchInvokeEvent, results *Results, handlers []BatchInvokePostHandler) *Results {
	for _, handler := range handlers {
		if err := handler(ctx, event, results); err != nil {
			e.batchInvokeErrorHandler(ctx, event, err)
			return makeErrorResults(event.NSource, err)
		}
	}

	return nil
}

func (e *EventManager) Run(ctx context.Context, req *Request) (interface{}, error) {
	switch req.eventType {
	case eventBatchInvokeType:
		event := req.BatchInvokeEvent
		if mainHandler, ok := e.batchInvokeFields[event.Field]; ok {
			if xresult := e.runBatchInvokePreHandler(ctx, event, e.batchInvokePreHandlers); xresult != nil {
				return xresult.Results, nil
			}

			if xresult := e.runBatchInvokePreHandler(ctx, event, mainHandler.preHandlers); xresult != nil {
				return xresult.Results, nil
			}

			results := mainHandler.handler(ctx, event)
			if results == nil {
				results := makeErrorResults(event.NSource, ErrNoResult)
				e.batchInvokeErrorHandler(ctx, event, results.Error)

				return results.Results, nil
			}

			if results.Error != nil {
				e.batchInvokeErrorHandler(ctx, event, results.Error)
			}

			if xresult := e.runBatchInvokePostHandler(ctx, event, results, mainHandler.postHandlers); xresult != nil {
				return xresult.Results, nil
			}

			if xresult := e.runBatchInvokePostHandler(ctx, event, results, e.batchInvokePostHandlers); xresult != nil {
				return xresult.Results, nil
			}

			return results.Results, nil
		}

		err := ErrFuncNotFound(event.Field)
		e.batchInvokeErrorHandler(ctx, event, err)
		return makeErrorResults(event.NSource, err).Results, nil

	case eventInvokeType:
		event := req.InvokeEvent
		if mainHandler, ok := e.invokeFields[event.Function]; ok {
			if xresult := e.runInvokePreHandler(ctx, event, e.invokePreHandlers); xresult != nil {
				return xresult, nil
			}

			if xresult := e.runInvokePreHandler(ctx, event, mainHandler.preHandlers); xresult != nil {
				return xresult, nil
			}

			result := mainHandler.handler(ctx, event)
			if result == nil {
				result := &Result{
					Data:  nil,
					Error: ErrNoResult,
				}
				e.invokeErrorHandler(ctx, event, result.Error)

				return result, nil
			}

			if result.Error != nil {
				e.invokeErrorHandler(ctx, event, result.Error)
			}

			if xresult := e.runInvokePostHandler(ctx, event, result, mainHandler.postHandlers); xresult != nil {
				return xresult, nil
			}

			if xresult := e.runInvokePostHandler(ctx, event, result, e.invokePostHandlers); xresult != nil {
				return xresult, nil
			}

			return result, nil
		}

		err := ErrFuncNotFound(event.Function)
		e.invokeErrorHandler(ctx, event, err)
		return &Result{
			Error: err,
		}, nil
	}

	return nil, errors.InternalErrorf("FIELD_NOT_FOUND", "Not found handler")
}
