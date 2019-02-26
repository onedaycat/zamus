package command

import (
	"context"
)

type commandinfo struct {
	handler     CommandHandler
	prehandlers []CommandHandler
}

func WithPermission(pm string) CommandHandler {
	return func(ctx context.Context, cmd *Command) (interface{}, error) {
		if cmd.Identity == nil {
			return nil, ErrPermissionDenied
		}

		if cmd.Identity.Claims.Permissions == nil {
			return nil, ErrPermissionDenied
		}

		if ok := cmd.Identity.Claims.Permissions.Has(cmd.PermissionKey, pm); !ok {
			return nil, ErrPermissionDenied
		}

		return nil, nil
	}
}
