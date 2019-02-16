package command

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/zamus/invoke"
)

type ErrorHandler func(ctx context.Context, event *invoke.InvokeEvent, err error)

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

func (h *Handler) PreHandler(handlers ...CommandHandler) {
	h.preHandlers = handlers
}

func (h *Handler) PostHandler(handlers ...CommandHandler) {
	h.postHandlers = handlers
}

func (h *Handler) ErrorHandler(handlers ...ErrorHandler) {
	h.errHandlers = handlers
}

func (h *Handler) RegisterCommand(command string, handler CommandHandler, prehandlers ...CommandHandler) {
	h.commands[command] = &commandinfo{
		handler:     handler,
		prehandlers: prehandlers,
	}
}

func (h *Handler) handler(ctx context.Context, event *invoke.InvokeEvent) (interface{}, error) {
	info, ok := h.commands[event.Function]
	if !ok {
		return nil, ErrCommandNotFound(event.Function)
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
