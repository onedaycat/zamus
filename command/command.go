package command

import (
	"context"

	"github.com/onedaycat/gocqrs/invoke"
)

type CommandHandler func(ctx context.Context, event *invoke.InvokeEvent) (interface{}, error)

type commandinfo struct {
	handler    CommandHandler
	permission map[string]struct{}
}

type CommandOptions func(info *commandinfo)

func WithPermission(pms ...string) CommandOptions {
	return func(info *commandinfo) {
		info.permission = make(map[string]struct{})
		for _, p := range pms {
			info.permission[p] = struct{}{}
		}
	}
}
