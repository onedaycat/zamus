package command

import (
	"context"
)

type commandinfo struct {
	handler     CommandHandler
	prehandlers []CommandHandler
}

func WithPermission(pm string) CommandHandler {
	return func(ctx context.Context, event *Command) (interface{}, error) {
		if event.Identity.Claims.Permissions == nil {
			return nil, ErrPermissionDenied
		}

		if ok := event.Identity.Claims.Permissions.Has(event.PermissionKey, pm); !ok {
			return nil, ErrPermissionDenied
		}

		return nil, nil
	}
}
