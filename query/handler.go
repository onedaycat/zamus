package query

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/zamus/eventstore"
)

type ErrorHandler func(ctx context.Context, queries *Query, err error)
type QueryHandler func(ctx context.Context, queries *Query) (QueryResult, error)
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

type Handler struct {
	quries       map[string]*queryinfo
	preHandlers  []QueryHandler
	postHandlers []QueryHandler
	errHandlers  []ErrorHandler
}

func NewHandler() *Handler {
	return &Handler{
		quries: make(map[string]*queryinfo, 30),
	}
}

func (h *Handler) PreHandler(handlers ...QueryHandler) {
	h.preHandlers = handlers
}

func (h *Handler) PostHandler(handlers ...QueryHandler) {
	h.postHandlers = handlers
}

func (h *Handler) ErrorHandler(handlers ...ErrorHandler) {
	h.errHandlers = handlers
}

func (h *Handler) RegisterQuery(query string, handler QueryHandler, prehandlers ...QueryHandler) {
	h.quries[query] = &queryinfo{
		handler:     handler,
		prehandlers: prehandlers,
	}
}

func (h *Handler) handler(ctx context.Context, event *Query) (QueryResult, error) {
	if event == nil {
		return nil, nil
	}

	info, ok := h.quries[event.Function]
	if !ok {
		return nil, ErrQueryNotFound(event.Function)
	}

	for _, handler := range h.preHandlers {
		result, err := handler(ctx, event)
		if err != nil {
			err = makeError(err)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, event, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	for _, handler := range info.prehandlers {
		result, err := handler(ctx, event)
		if err != nil {
			err = makeError(err)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, event, err)
			}
			return nil, err
		}

		if result.Len() != event.NSource {
			return nil, ErrQueryResultSizeNotMatch.WithCaller()
		}

		if result != nil {
			return result, nil
		}
	}

	result, err := info.handler(ctx, event)
	if err != nil {
		return nil, makeError(err)
	}

	for _, handler := range h.postHandlers {
		result, err := handler(ctx, event)
		if err != nil {
			err = makeError(err)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, event, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) StartLambda() {
	lambda.Start(h.handler)
}
