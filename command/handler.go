package command

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/onedaycat/gocqrs/invoke"
)

type Handler struct {
	commands map[string]*commandinfo
}

func NewHandler() *Handler {
	return &Handler{
		commands: make(map[string]*commandinfo, 30),
	}
}

func (h *Handler) RegisterCommand(command string, handler CommandHandler, options ...CommandOptions) {
	cmdinfo := &commandinfo{
		handler: handler,
	}

	for _, op := range options {
		op(cmdinfo)
	}

	h.commands[command] = cmdinfo
}

func (h *Handler) handler(ctx context.Context, event *invoke.InvokeEvent) (interface{}, error) {
	info, ok := h.commands[event.Function]
	if !ok {
		return nil, ErrCommandNotFound(event.Function)
	}

	result, err := info.handler(ctx, event)
	if err != nil {
		return nil, makeError(err)
	}

	return result, nil
}

func (h *Handler) StartLambda() {
	lambda.Start(h.handler)
}
