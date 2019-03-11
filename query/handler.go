package query

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type ErrorHandler func(ctx context.Context, query *Query, appErr errors.Error)
type QueryHandler func(ctx context.Context, query *Query) (QueryResult, errors.Error)
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

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
	warmer       *warmer.Warmer
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
			*err = appErr.ErrInternalError.WithCause(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, query, *err)
			}
			tracer.AddError(seg, *err)
		default:
			*err = appErr.ErrInternalError.WithInput(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, query, *err)
			}
			tracer.AddError(seg, *err)
		}
	}
}

func (h *Handler) doHandler(info *queryinfo, ctx context.Context, query *Query) (result QueryResult, err errors.Error) {
	hctx, seg := tracer.BeginSubsegment(ctx, "handler")
	defer h.recovery(hctx, query, &err)
	defer seg.Close(nil)
	result, err = info.handler(hctx, query)
	if err != nil {
		for _, errHandler := range h.errHandlers {
			errHandler(hctx, query, err)
		}
		tracer.AddError(seg, err)
		return nil, err
	}

	if query.NBatchSources > 0 && result != nil && result.Len() != query.NBatchSources {
		err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
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
			err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
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
			err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
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
			err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(query)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) runWarmer(ctx context.Context, query *Query) (QueryResult, errors.Error) {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}
		h.warmer = warmer.New(sess)
	}
	h.warmer.Run(ctx, query.Concurency, query.CorrelationID)

	return nil, nil
}

func (h *Handler) Handle(ctx context.Context, query *Query) (QueryResult, errors.Error) {
	if query == nil {
		return nil, appErr.ErrUnableParseQuery
	}
	if query.Warmer {
		return h.runWarmer(ctx, query)
	}

	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	info, ok := h.quries[query.Function]
	if !ok {
		return nil, appErr.ErrQueryNotFound(query.Function)
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
