package command

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/onedaycat/errors/sentry"
	"github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/tracer"
	"github.com/onedaycat/zamus/zamuscontext"
)

type Command = invoke.InvokeEvent
type CommandInput = invoke.InvokeRequest

type ErrorHandler func(ctx context.Context, cmd *Command, err errors.Error)
type CommandHandler func(ctx context.Context, cmd *Command) (interface{}, errors.Error)
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

func (h *Handler) recovery(ctx context.Context, cmd *Command, err *errors.Error) {
	if r := recover(); r != nil {
		seg := tracer.GetSegment(ctx)
		defer tracer.Close(seg)
		switch cause := r.(type) {
		case error:
			*err = errors.ErrPanic.WithCause(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, cmd, *err)
			}
			tracer.AddError(seg, *err)
		default:
			*err = errors.ErrPanic.WithInput(cause).WithCallerSkip(6).WithPanic()
			for _, errhandler := range h.errHandlers {
				errhandler(ctx, cmd, *err)
			}
			tracer.AddError(seg, *err)
		}
	}
}

func (h *Handler) doHandler(info *commandinfo, ctx context.Context, cmd *Command) (result interface{}, err errors.Error) {
	hctx, seg := tracer.BeginSubsegment(ctx, "handler")
	defer tracer.Close(seg)
	defer h.recovery(hctx, cmd, &err)
	result, err = info.handler(hctx, cmd)
	if err != nil {
		err.WithCaller().WithInput(cmd)
		for _, errHandler := range h.errHandlers {
			errHandler(hctx, cmd, err)
		}
		tracer.AddError(seg, err)
		return nil, err
	}

	return result, nil
}

func (h *Handler) doPreHandler(ctx context.Context, cmd *Command) (result interface{}, err errors.Error) {
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

func (h *Handler) doInPreHandler(info *commandinfo, ctx context.Context, cmd *Command) (result interface{}, err errors.Error) {
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

func (h *Handler) doPostHandler(ctx context.Context, cmd *Command) (result interface{}, err errors.Error) {
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

func (h *Handler) Handle(ctx context.Context, cmd *Command) (interface{}, errors.Error) {
	info, ok := h.commands[cmd.Function]
	if !ok {
		return nil, errors.ErrCommandNotFound(cmd.Function)
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

func (h *Handler) StartLambda() {
	lambda.Start(h.Handle)
}
