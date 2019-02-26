package command

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/invoke"
)

type Command = invoke.InvokeEvent

type ErrorHandler func(ctx context.Context, cmd *Command, err error)
type CommandHandler func(ctx context.Context, cmd *Command) (interface{}, error)
type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

func init() {
	common.PrettyLog()
}

type Handler struct {
	commands     map[string]*commandinfo
	preHandlers  []CommandHandler
	postHandlers []CommandHandler
	errHandlers  []ErrorHandler
}

func NewHandler() *Handler {
	return &Handler{
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

func (h *Handler) recovery(ctx context.Context, cmd *Command, err *error) {
	if r := recover(); r != nil {
		cause, ok := r.(error)
		if ok {
			appErr := errors.InternalError("PANIC", "Server Error").WithCause(cause).WithCallerSkip(4)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, cmd, appErr)
			}
			*err = appErr
		}
	}
}

func (h *Handler) doHandler(info *commandinfo, ctx context.Context, cmd *Command) (result interface{}, err error) {
	defer h.recovery(ctx, cmd, &err)
	result, err = info.handler(ctx, cmd)
	if err != nil {
		err = makeError(err)
		for _, errHandler := range h.errHandlers {
			errHandler(ctx, cmd, err)
		}
		return nil, err
	}

	return
}

func (h *Handler) handler(ctx context.Context, cmd *Command) (interface{}, error) {
	info, ok := h.commands[cmd.Function]
	if !ok {
		return nil, ErrCommandNotFound(cmd.Function)
	}

	for _, handler := range h.preHandlers {
		result, err := handler(ctx, cmd)
		if err != nil {
			err = makeError(err)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, cmd, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	for _, handler := range info.prehandlers {
		result, err := handler(ctx, cmd)
		if err != nil {
			err = makeError(err)
			for _, errHandler := range h.errHandlers {
				errHandler(ctx, cmd, err)
			}
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	result, err := h.doHandler(info, ctx, cmd)
	if err != nil {
		return nil, err
	}

	for _, handler := range h.postHandlers {
		result, err := handler(ctx, cmd)
		if err != nil {
			err = makeError(err)
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

func (h *Handler) StartLambda() {
	lambda.Start(h.handler)
}
