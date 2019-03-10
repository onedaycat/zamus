package command

import (
	"context"

	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
)

type commandinfo struct {
	handler     CommandHandler
	prehandlers []CommandHandler
}

func WithPermission(pm string) CommandHandler {
	return func(ctx context.Context, cmd *Command) (interface{}, errors.Error) {
		if cmd.Identity == nil {
			return nil, appErr.ErrPermissionDenied
		}

		if cmd.Identity.Claims.Permissions == nil {
			return nil, appErr.ErrPermissionDenied
		}

		if ok := cmd.Identity.Claims.Permissions.Has(cmd.PermissionKey, pm); !ok {
			return nil, appErr.ErrPermissionDenied
		}

		return nil, nil
	}
}
