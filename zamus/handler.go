package zamus

import (
    "context"
    "encoding/json"
    "fmt"

    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
)

const (
    firstCharArray  = 91
    firstCharObject = 123
)

var jsonen = jsoniter.ConfigCompatibleWithStandardLibrary

type SourceList interface{}

type PreHandler func(ctx context.Context, src interface{}) (interface{}, error)
type PostHandler func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error)
type BatchPreHandler func(ctx context.Context, src interface{}) (interface{}, error)
type BatchPostHandler func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error)
type PanicHandler func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error)
type RetryFailedHandler func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error)

type Handler interface {
    ParseSource(ctx context.Context, payload json.RawMessage) interface{}
    ParseSources(ctx context.Context, payload json.RawMessage) interface{}
    Handler(ctx context.Context, source interface{}) (interface{}, error)
    BatchHandler(ctx context.Context, sources interface{}) (interface{}, error)
}

type Handle struct {
    retries           *Retries
    handle            Handler
    preHandlers       []PreHandler
    postHandlers      []PostHandler
    batchPreHandlers  []BatchPreHandler
    batchPostHandlers []BatchPostHandler
    panicHandler      PanicHandler
    retryHandler      RetryFailedHandler
}

func New(handle Handler) *Handle {
    return &Handle{
        handle:  handle,
        retries: NewRetries(0),
    }
}

func (h *Handle) SetRetry(times int) {
    h.retries.SetTimes(times)
}

func (h *Handle) Invoke(ctx context.Context, payload json.RawMessage) (result interface{}, err error) {
    var src interface{}
    var isBatch bool
    defer h.recovery(ctx, payload, &result, &err)

    src, isBatch = h.parseSource(ctx, payload)
    result, err = h.Run(ctx, payload, src, isBatch)

    return result, err
}

func (h *Handle) Run(ctx context.Context, payload json.RawMessage, src interface{}, isBatch bool) (interface{}, error) {
    h.retries.Reset()

    if isBatch {
        result, err := h.doBatchPreHandler(ctx, src)
        if err != nil || result != nil {
            return result, err
        }
    RetryBatchHandler:
        result, err = h.handle.BatchHandler(ctx, src)
        if err != nil {
            if h.retries.Retry() {
                goto RetryBatchHandler
            } else {
                if h.retryHandler != nil {
                    result, err = h.retryHandler(ctx, payload, err)
                }
            }
        }

        result, err = h.doBatchPostHandler(ctx, src, result, err)

        return result, err
    }

    result, err := h.doPreHandler(ctx, src)
    if err != nil || result != nil {
        return result, err
    }

RetryHandler:
    result, err = h.handle.Handler(ctx, src)
    if err != nil {
        if h.retries.Retry() {
            goto RetryHandler
        } else {
            if h.retryHandler != nil {
                result, err = h.retryHandler(ctx, payload, err)
            }
        }
    }

    result, err = h.doPostHandler(ctx, src, result, err)

    return result, err
}

func (h *Handle) RegisterPreHandler(preHandlers ...PreHandler) {
    h.preHandlers = append(h.preHandlers, preHandlers...)
}

func (h *Handle) RegisterPostHandler(postHandlers ...PostHandler) {
    h.postHandlers = append(h.postHandlers, postHandlers...)
}

func (h *Handle) RegisterBatchPreHandler(batchPreHandlers ...BatchPreHandler) {
    h.batchPreHandlers = append(h.batchPreHandlers, batchPreHandlers...)
}

func (h *Handle) RegisterBatchPostHandler(batchPostHandlers ...BatchPostHandler) {
    h.batchPostHandlers = append(h.batchPostHandlers, batchPostHandlers...)
}

func (h *Handle) OnPanicHandler(panicHandler PanicHandler) {
    h.panicHandler = panicHandler
}

func (h *Handle) OnRetryFailedHandler(retryFailedHandler RetryFailedHandler) {
    h.retryHandler = retryFailedHandler
}

func (h *Handle) doPreHandler(ctx context.Context, src interface{}) (interface{}, error) {
    for _, ph := range h.preHandlers {
        result, err := ph(ctx, src)
        if err != nil || result != nil {
            return result, err
        }
    }

    return nil, nil
}

func (h *Handle) doBatchPreHandler(ctx context.Context, src interface{}) (interface{}, error) {
    for _, ph := range h.batchPreHandlers {
        result, err := ph(ctx, src)
        if err != nil || result != nil {
            return result, err
        }
    }

    return nil, nil
}

func (h *Handle) doPostHandler(ctx context.Context, src interface{}, res interface{}, reserr error) (interface{}, error) {
    result := res
    err := reserr

    for _, ph := range h.postHandlers {
        result, err = ph(ctx, src, result, err)
    }

    return result, err
}

func (h *Handle) doBatchPostHandler(ctx context.Context, src interface{}, res interface{}, reserr error) (interface{}, error) {
    result := res
    err := reserr

    for _, ph := range h.batchPostHandlers {
        result, err = ph(ctx, src, result, err)
    }

    return result, err
}

func (h *Handle) parseSource(ctx context.Context, payload json.RawMessage) (interface{}, bool) {
    firstChar := payload[0]
    if firstChar == firstCharArray {
        sources := h.handle.ParseSources(ctx, payload)
        return sources, true
    } else if firstChar == firstCharObject {
        source := h.handle.ParseSource(ctx, payload)
        return source, false
    }

    panic(errors.InternalError("UnableParseRequest", "Unable to parse request"))
}

func (h *Handle) recovery(ctx context.Context, payload json.RawMessage, result *interface{}, err *error) {
    if r := recover(); r != nil {
        switch cause := r.(type) {
        case errors.Error:
            *err = cause.WithPanic()
        case error:
            *err = errors.InternalError(GetErrorType(err), cause.Error()).WithPanic()
        default:
            *err = errors.InternalError(GetErrorType(err), fmt.Sprintf("%v", cause)).WithPanic()
        }

        if h.panicHandler != nil {
            *result, *err = h.panicHandler(ctx, payload, *err)
        }
    }
}
