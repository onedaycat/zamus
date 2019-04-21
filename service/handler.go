package service

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	ldService "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/sentry"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type ErrorHandler func(ctx context.Context, req *Request, err errors.Error)
type Handler func(ctx context.Context, req *Request) (interface{}, errors.Error)
type MergeBatchHandler func(ctx context.Context, req *Request, results BatchResults) errors.Error

type Config struct {
	AppStage    string
	Service     string
	Version     string
	SentryDNS   string
	EnableTrace bool
}

//noinspection GoNameStartsWithPackageName
type ServiceHandler struct {
	handlers     map[string]*handlerinfo
	preHandlers  []Handler
	postHandlers []Handler
	errHandlers  []ErrorHandler
	zcctx        *zamuscontext.ZamusContext
	warmer       *warmer.Warmer
	req          *mainReq
}

func NewHandler(config *Config) *ServiceHandler {
	h := &ServiceHandler{
		zcctx: &zamuscontext.ZamusContext{
			AppStage:       config.AppStage,
			Service:        config.Service,
			LambdaFunction: lambdacontext.FunctionName,
			LambdaVersion:  lambdacontext.FunctionVersion,
			Version:        config.Version,
		},
		handlers: make(map[string]*handlerinfo, 30),
		req: &mainReq{
			reqs:        make([]*Request, 0, 10),
			req:         &Request{},
			batchResult: make([]*BatchResult, 0, 10),
		},
	}

	for i := 0; i < 10; i++ {
		h.req.batchResult = append(h.req.batchResult, &BatchResult{})
	}

	if config.EnableTrace {
		tracer.Enable = config.EnableTrace
		h.ErrorHandlers(TraceError)
	}

	if config.SentryDNS != "" {
		sentry.SetDSN(config.SentryDNS)
		sentry.SetOptions(
			sentry.WithEnv(config.AppStage),
			sentry.WithServerName(lambdacontext.FunctionName),
			sentry.WithServiceName(config.Service),
			sentry.WithRelease(config.Service+"@"+config.Version),
			sentry.WithVersion(config.Version),
			sentry.WithTags(sentry.Tags{
				{Key: "lambdaVersion", Value: lambdacontext.FunctionVersion},
			}),
		)
		h.ErrorHandlers(Sentry)
	}

	return h
}

func (h *ServiceHandler) PreHandlers(handlers ...Handler) {
	h.preHandlers = append(h.preHandlers, handlers...)
}

func (h *ServiceHandler) PostHandlers(handlers ...Handler) {
	h.postHandlers = append(h.postHandlers, handlers...)
}

func (h *ServiceHandler) ErrorHandlers(handlers ...ErrorHandler) {
	h.errHandlers = append(h.errHandlers, handlers...)
}

func (h *ServiceHandler) RegisterHandler(name string, handler Handler, options ...HandlerOption) {
	opts := &handlerOptions{}

	for _, option := range options {
		option(opts)
	}

	info := &handlerinfo{
		name:         name,
		handler:      handler,
		prehandlers:  opts.prehandlers,
		mergeHandler: opts.mergeHandler,
		reqs:         make([]*Request, 0, 10),
		batchResult:  make(BatchResults, 10),
	}

	for i := 0; i < 10; i++ {
		info.batchResult[i] = &BatchResult{}
	}

	h.handlers[name] = info
}

func (h *ServiceHandler) recovery(ctx context.Context, req *Request, err *errors.Error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(req)
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, req, *err)
			}
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(req)
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, req, *err)
			}
		}
	}
}

func (h *ServiceHandler) doHandler(info *handlerinfo, ctx context.Context, req *Request) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	result, err = info.handler(ctx, req)
	if err != nil {
		for _, errHandler := range h.errHandlers {
			errHandler(ctx, req, err)
		}
		return nil, err
	}

	return result, nil
}

func (h *ServiceHandler) doPreHandler(ctx context.Context, req *Request) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	for _, handler := range h.preHandlers {
		result, err = handler(ctx, req)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, req, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *ServiceHandler) doInPreHandler(info *handlerinfo, ctx context.Context, req *Request) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	for _, handler := range info.prehandlers {
		result, err = handler(ctx, req)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, req, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *ServiceHandler) doPostHandler(ctx context.Context, req *Request) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, req, &err)
	for _, handler := range h.postHandlers {
		result, err = handler(ctx, req)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, req, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *ServiceHandler) runWarmer(ctx context.Context, req *Request) (interface{}, errors.Error) {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}

		h.warmer = warmer.New(ldService.New(sess))
	}
	h.warmer.Run(ctx, req.Concurency)

	return nil, nil
}

func (h *ServiceHandler) runHandler(info *handlerinfo, ctx context.Context, req *Request) (interface{}, errors.Error) {
	result, err := h.doPreHandler(ctx, req)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil

	}

	result, err = h.doInPreHandler(info, ctx, req)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil

	}

	result, err = h.doHandler(info, ctx, req)
	if err != nil {
		return nil, err
	}

	postresult, err := h.doPostHandler(ctx, req)
	if err != nil {
		return nil, err
	}
	if postresult != nil {
		return postresult, nil
	}

	return result, nil
}

func (h *ServiceHandler) Handle(ctx context.Context, req *Request) (interface{}, errors.Error) {
	if req == nil {
		return nil, appErr.ErrUnableParseRequest
	}
	if req.Warmer {
		return h.runWarmer(ctx, req)
	}

	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	info, ok := h.handlers[req.Method]
	if !ok {
		return nil, appErr.ErrFunctionNotFound(req.Method)
	}

	return h.runHandler(info, zmctx, req)
}

func (h *ServiceHandler) Run(ctx context.Context, req *Request, result interface{}) error {
	reqByte, err := common.MarshalJSON(req)
	if err != nil {
		panic(err)
	}

	resByte, xerr := h.Invoke(ctx, reqByte)
	if xerr != nil {
		return xerr
	}

	if resByte != nil {
		if err := common.UnmarshalJSON(resByte, result); err != nil {
			panic(err)
		}
	}

	return xerr
}

func (h *ServiceHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	if err := h.req.UnmarshalRequest(payload); err != nil {
		return nil, err
	}

	if h.req.totalIndex > 0 {
		return h.BatchHandle(ctx, h.req.reqs).MarshalResponses()
	}

	result, err := h.Handle(ctx, h.req.req)
	if err != nil {
		return nil, appErr.ToLambdaError(err)
	}

	var resultByte []byte
	if result != nil {
		resultByte, _ = common.MarshalJSON(result)
		return resultByte, nil
	}

	return nil, nil
}

func (h *ServiceHandler) StartLambda() {
	lambda.StartHandler(h)
}

type handlerinfo struct {
	name         string
	handler      Handler
	prehandlers  []Handler
	mergeHandler MergeBatchHandler
	reqs         []*Request
	batchResult  BatchResults
}

func (q *handlerinfo) mergeReq(req *Request) []int {
	b := bytes.NewBuffer(nil)
	b.WriteByte(91)
	first := true
	n := 0
	reqIndex := make([]int, 0, len(q.reqs))

	for i := 0; i < len(q.reqs); i++ {
		if len(q.reqs[i].Input) == 0 {
			continue
		}

		if !first {
			b.WriteByte(44)
		}
		b.Write(q.reqs[i].Input)
		first = false
		n = n + 1
		reqIndex = append(reqIndex, q.reqs[i].index)
	}
	b.WriteByte(93)

	req.Method = q.reqs[0].Method
	req.Input = b.Bytes()
	req.Identity = q.reqs[0].Identity

	if len(req.Input) == 2 {
		req.Input = nil
	}

	return reqIndex
}
