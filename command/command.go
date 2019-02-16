package command

import (
	"context"

	"github.com/onedaycat/zamus/invoke"
)

type CommandHandler func(ctx context.Context, event *invoke.InvokeEvent) (interface{}, error)

type commandinfo struct {
	handler     CommandHandler
	prehandlers []CommandHandler
}

func WithPermission(pm string) CommandHandler {
	return func(ctx context.Context, event *invoke.InvokeEvent) (interface{}, error) {
		if event.Identity.Permissions == nil {
			return nil, ErrPermissionDenied
		}

		if ok := event.Identity.Permissions.Has(event.PermissionKey, pm); !ok {
			return nil, ErrPermissionDenied
		}

		return nil, nil
	}
}
