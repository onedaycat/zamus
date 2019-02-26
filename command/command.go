package command

import (
	"context"

	"github.com/onedaycat/zamus/errors"
)

type commandinfo struct {
	handler     CommandHandler
	prehandlers []CommandHandler
}

func WithPermission(pm string) CommandHandler {
	return func(ctx context.Context, cmd *Command) (interface{}, error) {
		if cmd.Identity == nil {
			return nil, errors.ErrPermissionDenied
		}

		if cmd.Identity.Claims.Permissions == nil {
			return nil, errors.ErrPermissionDenied
		}

		if ok := cmd.Identity.Claims.Permissions.Has(cmd.PermissionKey, pm); !ok {
			return nil, errors.ErrPermissionDenied
		}

		return nil, nil
	}
}
