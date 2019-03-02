package query

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	errs "github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
)

type ErrorHandler func(ctx context.Context, query *Query, err error)
type QueryHandler func(ctx context.Context, query *Query) (QueryResult, error)
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

func init() {
	common.PrettyLog()
	xray.SetLogger(xraylog.NullLogger)
}

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

func (h *Handler) PreHandlers(handlers ...QueryHandler) {
	h.preHandlers = handlers
}

func (h *Handler) PostHandlers(handlers ...QueryHandler) {
	h.postHandlers = handlers
}

func (h *Handler) ErrorHandlers(handlers ...ErrorHandler) {
	h.errHandlers = handlers
}

func (h *Handler) RegisterQuery(query string, handler QueryHandler, prehandlers ...QueryHandler) {
	h.quries[query] = &queryinfo{
		handler:     handler,
		prehandlers: prehandlers,
	}
}

func (h *Handler) recovery(ctx context.Context, query *Query, err *error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			appErr := errors.ErrPanic.WithCause(cause).WithCallerSkip(6)
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, query, appErr)
			}
			*err = appErr
		case string:
			appErr := errors.ErrPanic.WithCause(errs.New(cause)).WithCallerSkip(6)
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, query, appErr)
			}
			*err = appErr
		default:
			panic(cause)
		}
	}
}

func (h *Handler) doHandler(info *queryinfo, ctx context.Context, query *Query) (result QueryResult, err error) {
	defer h.recovery(ctx, query, &err)
	result, err = info.handler(ctx, query)
	if err != nil {
		err = errors.Warp(err).WithCaller().WithInput(query)
		for _, errHandler := range h.errHandlers {
			errHandler(ctx, query, err)
		}
		return nil, err
	}

	if query.NBatchSources > 0 && result.Len() != query.NBatchSources {
		return nil, errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
	}

	return
}

func (h *Handler) Handle(ctx context.Context, query *Query) (QueryResult, error) {
	if query == nil {
		return nil, errors.ErrUnableParseQuery
	}

	info, ok := h.quries[query.Function]
	if !ok {
		return nil, errors.ErrQueryNotFound(query.Function)
	}

	for _, handler := range h.preHandlers {
		result, err := handler(ctx, query)
		if err != nil {
			err = errors.Warp(err).WithCaller().WithInput(query)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, query, err)
			}
			return nil, err
		}

		if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
			return nil, errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
		}

		if result != nil {
			return result, nil
		}
	}

	for _, handler := range info.prehandlers {
		result, err := handler(ctx, query)
		if err != nil {
			err = errors.Warp(err).WithCaller().WithInput(query)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, query, err)
			}
			return nil, err
		}

		if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
			return nil, errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
		}

		if result != nil {
			return result, nil
		}
	}

	result, err := h.doHandler(info, ctx, query)
	if err != nil {
		return nil, err
	}

	for _, handler := range h.postHandlers {
		result, err := handler(ctx, query)
		if err != nil {
			err = errors.Warp(err).WithCaller().WithInput(query)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, query, err)
			}
			return nil, err
		}

		if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
			return nil, errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) StartLambda() {
	lambda.Start(h.Handle)
}
