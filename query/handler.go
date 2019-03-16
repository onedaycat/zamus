package query

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type ErrorHandler func(ctx context.Context, req *QueryReq, appErr errors.Error)
type QueryHandler func(ctx context.Context, req *QueryReq) (QueryResult, errors.Error)
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

func (h *Handler) recovery(ctx context.Context, req *QueryReq, err *errors.Error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(req)
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, req, *err)
			}
			tracer.AddError(ctx, *err)
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(req)
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, req, *err)
			}
			tracer.AddError(ctx, *err)
		}
	}
}

func (h *Handler) doHandler(info *queryinfo, ctx context.Context, req *QueryReq) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	result, err = info.handler(ctx, req)
	if err != nil {
		for _, errHandler := range h.errHandlers {
			errHandler(ctx, req, err)
		}
		tracer.AddError(ctx, err)
		return nil, err
	}

	if req.NBatchSources > 0 && result != nil && result.Len() != req.NBatchSources {
		err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(req)
		tracer.AddError(ctx, err)
		return nil, err
	}

	return result, nil
}

func (h *Handler) doPreHandler(ctx context.Context, req *QueryReq) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	for _, handler := range h.preHandlers {
		result, err = handler(ctx, req)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, req, err)
			}
			return nil, err
		}

		if req.NBatchSources > 0 && result != nil && result.Len() != req.NBatchSources {
			err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(req)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) doInPreHandler(info *queryinfo, ctx context.Context, req *QueryReq) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	for _, handler := range info.prehandlers {
		result, err = handler(ctx, req)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, req, err)
			}
			return nil, err
		}

		if req.NBatchSources > 0 && result != nil && result.Len() != req.NBatchSources {
			err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(req)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) doPostHandler(ctx context.Context, req *QueryReq) (result QueryResult, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	for _, handler := range h.postHandlers {
		result, err = handler(ctx, req)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, req, err)
			}
			return nil, err
		}

		if req.NBatchSources > 0 && result != nil && result.Len() != req.NBatchSources {
			err = appErr.ErrQueryResultSizeNotMatch.WithCaller().WithInput(req)
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) runWarmer(ctx context.Context, req *QueryReq) (QueryResult, errors.Error) {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}
		h.warmer = warmer.New(sess)
	}
	h.warmer.Run(ctx, req.Concurency)

	return nil, nil
}

func (h *Handler) Handle(ctx context.Context, req *QueryReq) (QueryResult, errors.Error) {
	if req == nil {
		return nil, appErr.ErrUnableParseQuery
	}
	if req.Warmer {
		return h.runWarmer(ctx, req)
	}

	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	info, ok := h.quries[req.Function]
	if !ok {
		return nil, appErr.ErrQueryNotFound(req.Function)
	}

	result, err := h.doPreHandler(zmctx, req)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	result, err = h.doInPreHandler(info, zmctx, req)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	result, err = h.doHandler(info, zmctx, req)
	if err != nil {
		return nil, err
	}

	postresult, err := h.doPostHandler(zmctx, req)
	if err != nil {
		return nil, err
	}
	if postresult != nil {
		return postresult, nil
	}

	return result, nil
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	req := &QueryReq{}
	if err := req.UnmarshalRequest(payload); err != nil {
		return nil, err
	}

	result, err := h.Handle(ctx, req)
	if err != nil {
		return nil, err
	}

	resultByte, _ := common.MarshalJSON(result)

	return resultByte, nil
}

func (h *Handler) StartLambda() {
	lambda.StartHandler(h)
}
