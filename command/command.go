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
	return func(ctx context.Context, req *CommandReq) (interface{}, errors.Error) {
		if req.Identity == nil {
			return nil, appErr.ErrPermissionDenied
		}

		if req.Identity.Claims.Permissions == nil {
			return nil, appErr.ErrPermissionDenied
		}

		if ok := req.Identity.Claims.Permissions.Has(req.PermissionKey, pm); !ok {
			return nil, appErr.ErrPermissionDenied
		}

		return nil, nil
	}
}
