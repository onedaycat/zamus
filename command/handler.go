package command

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
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/warmer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type Identity = invoke.Identity
type NewAggregateFn = eventstore.NewAggregateFn
type ErrorHandler func(ctx context.Context, cmd *CommandReq, err errors.Error)
type CommandHandler func(ctx context.Context, cmd *CommandReq) (interface{}, errors.Error)
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
	commands     map[string]*commandinfo
	preHandlers  []CommandHandler
	postHandlers []CommandHandler
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
		commands: make(map[string]*commandinfo, 30),
	}
}

func (h *Handler) PreHandlers(handlers ...CommandHandler) {
	h.preHandlers = handlers
}

func (h *Handler) PostHandlers(handlers ...CommandHandler) {
	h.postHandlers = handlers
}

func (h *Handler) ErrorHandlers(handlers ...ErrorHandler) {
	h.errHandlers = handlers
}

func (h *Handler) RegisterCommand(command string, handler CommandHandler, prehandlers ...CommandHandler) {
	h.commands[command] = &commandinfo{
		handler:     handler,
		prehandlers: prehandlers,
	}
}

func (h *Handler) recovery(ctx context.Context, cmd *CommandReq, err *errors.Error) {
	if r := recover(); r != nil {
		switch cause := r.(type) {
		case error:
			*err = appErr.ErrPanic.WithCause(cause).WithCaller()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, cmd, *err)
			}
			tracer.AddError(ctx, *err)
		default:
			*err = appErr.ErrPanic.WithPanic().WithMessage(fmt.Sprintf("%v\n", cause)).WithCaller()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, cmd, *err)
			}
			tracer.AddError(ctx, *err)
		}
	}
}

func (h *Handler) doHandler(info *commandinfo, ctx context.Context, cmd *CommandReq) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, cmd, &err)
	result, err = info.handler(ctx, cmd)
	if err != nil {
		err.WithCaller().WithInput(cmd)
		for _, errHandler := range h.errHandlers {
			errHandler(ctx, cmd, err)
		}
		tracer.AddError(ctx, err)
		return nil, err
	}

	return result, nil
}

func (h *Handler) doPreHandler(ctx context.Context, cmd *CommandReq) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, cmd, &err)
	for _, handler := range h.preHandlers {
		result, err = handler(ctx, cmd)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, cmd, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) doInPreHandler(info *commandinfo, ctx context.Context, cmd *CommandReq) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, cmd, &err)
	for _, handler := range info.prehandlers {
		result, err = handler(ctx, cmd)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, cmd, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) doPostHandler(ctx context.Context, cmd *CommandReq) (result interface{}, err errors.Error) {
	defer h.recovery(ctx, cmd, &err)
	for _, handler := range h.postHandlers {
		result, err = handler(ctx, cmd)
		if err != nil {
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, cmd, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return result, nil
}

func (h *Handler) runWarmer(ctx context.Context, cmd *CommandReq) (interface{}, errors.Error) {
	if h.warmer == nil {
		sess, serr := session.NewSession()
		if serr != nil {
			panic(serr)
		}
		h.warmer = warmer.New(sess)
	}
	h.warmer.Run(ctx, cmd.Concurency)

	return nil, nil
}

func (h *Handler) Handle(ctx context.Context, cmd *CommandReq) (interface{}, errors.Error) {
	if cmd.Warmer {
		return h.runWarmer(ctx, cmd)
	}

	info, ok := h.commands[cmd.Function]
	if !ok {
		return nil, appErr.ErrCommandNotFound(cmd.Function)
	}

	zmctx := zamuscontext.NewContext(ctx, h.zcctx)

	result, err := h.doPreHandler(zmctx, cmd)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	result, err = h.doInPreHandler(info, zmctx, cmd)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	result, err = h.doHandler(info, zmctx, cmd)
	if err != nil {
		return nil, err
	}

	postresult, err := h.doPostHandler(zmctx, cmd)
	if err != nil {
		return nil, err
	}
	if postresult != nil {
		return postresult, nil
	}

	return result, nil
}

func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	req := &CommandReq{}
	if err := common.UnmarshalJSON(payload, req); err != nil {
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
