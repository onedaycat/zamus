package query

import (
	"context"

	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/zamuscontext"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/tracer"
)

type ErrorHandler func(ctx context.Context, query *Query, appErr errors.Error)
type QueryHandler func(ctx context.Context, query *Query) (QueryResult, errors.Error)
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

func init() {
	common.PrettyLog()
	xray.SetLogger(xraylog.NullLogger)
}

type Config struct {
	AppStage      string
	Service       string
	Version       string
	SentryRelease string
	SentryDNS     string
	EnableTrace   bool
}

type Handler struct {
	quries       map[string]*queryinfo
	preHandlers  []QueryHandler
	postHandlers []QueryHandler
	errHandlers  []ErrorHandler
	zcctx        *zamuscontext.ZamusContext
}

func NewHandler(config *Config) *Handler {
	if config.SentryDNS != "" {
		sentry.SetDSN(config.SentryDNS)
		sentry.SetOptions(
			sentry.WithEnv(config.AppStage),
			sentry.WithRelease(config.SentryRelease),
			sentry.WithServerName(lambdacontext.FunctionName),
			sentry.WithServiceName(config.Service),
			sentry.WithVersion(config.Version),
			sentry.WithTags(sentry.Tags{
				{"lambdaVersion", lambdacontext.FunctionVersion},
			}),
		)
	}

	tracer.Enable = config.EnableTrace

	return &Handler{
		zcctx: &zamuscontext.ZamusContext{
			AppStage:       config.AppStage,
			Service:        config.Service,
			LambdaFunction: lambdacontext.FunctionName,
			LambdaVersion:  lambdacontext.FunctionVersion,
			Version:        config.Version,
		},
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

func (h *Handler) recovery(ctx context.Context, query *Query, err *errors.Error) {
	if r := recover(); r != nil {
		seg := tracer.GetSegment(ctx)
		defer tracer.Close(seg)
		switch cause := r.(type) {
		case error:
			*err = errors.ErrPanic.WithCause(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, query, *err)
			}
			tracer.AddError(seg, *err)
		default:
			*err = errors.ErrPanic.WithInput(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, query, *err)
			}
			tracer.AddError(seg, *err)
		}
	}
}

func (h *Handler) doHandler(info *queryinfo, ctx context.Context, query *Query) (result QueryResult, err errors.Error) {
	hctx, seg := tracer.BeginSubsegment(ctx, "handler")
	defer seg.Close(nil)
	defer h.recovery(hctx, query, &err)
	result, err = info.handler(hctx, query)
	if err != nil {
		for _, errHandler := range h.errHandlers {
			errHandler(hctx, query, err)
		}
		tracer.AddError(seg, err)
		return nil, err
	}

	if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
		err = errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
		tracer.AddError(seg, err)
		return nil, err
	}

	return result, nil
}

func (h *Handler) doPreHandler(ctx context.Context, query *Query) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, query, &err)
	for _, handler := range h.preHandlers {
		result, err = handler(ctx, query)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, query, err)
			}
			return nil, err
		}

		if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
			err = errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) doInPreHandler(info *queryinfo, ctx context.Context, query *Query) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, query, &err)
	for _, handler := range info.prehandlers {
		result, err = handler(ctx, query)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, query, err)
			}
			return nil, err
		}

		if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
			err = errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) doPostHandler(ctx context.Context, query *Query) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, query, &err)
	for _, handler := range h.postHandlers {
		result, err = handler(ctx, query)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, query, err)
			}
			return nil, err
		}

		if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
			err = errors.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) Handle(ctx context.Context, query *Query) (QueryResult, error) {
	if query == nil {
		return nil, errors.ErrUnableParseQuery
	}

	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	info, ok := h.quries[query.Function]
	if !ok {
		return nil, errors.ErrQueryNotFound(query.Function)
	}

	result, err := h.doPreHandler(zmctx, query)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	result, err = h.doInPreHandler(info, zmctx, query)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	result, err = h.doHandler(info, zmctx, query)
	if err != nil {
		return nil, err
	}

	postresult, err := h.doPostHandler(zmctx, query)
	if err != nil {
		return nil, err
	}
	if postresult != nil {
		return postresult, nil
	}

	return result, nil
}

func (h *Handler) StartLambda() {
	lambda.Start(h.Handle)
}
