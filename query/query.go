package query

import (
	"bytes"
	"context"

	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
)

type QueryReq struct {
	Function      string              `json:"function"`
	Args          jsoniter.RawMessage `json:"arguments"`
	Sources       jsoniter.RawMessage `json:"source,omitempty"`
	Identity      *invoke.Identity    `json:"identity,omitempty"`
	NBatchSources int                 `json:"-"`
	PermissionKey string              `json:"pemKey,omitempty"`
	Warmer        bool                `json:"warmer,omitempty"`
	Concurency    int                 `json:"concurency,omitempty"`
}

func (e *QueryReq) ParseArgs(v interface{}) errors.Error {
	if err := common.UnmarshalJSON(e.Args, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *QueryReq) ParseSource(v interface{}) errors.Error {
	if err := common.UnmarshalJSON(e.Sources, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

type queryinfo struct {
	handler     QueryHandler
	prehandlers []QueryHandler
}

func WithPermission(pm string) QueryHandler {
	return func(ctx context.Context, req *QueryReq) (QueryResult, errors.Error) {
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

const (
	firstCharArray  = 91
	firstCharObject = 123
)

type QueryResult interface {
	Len() int
}

func (q *QueryReq) UnmarshalRequest(b []byte) error {
	var err error
	firstChar := b[0]

	if firstChar == firstCharArray {
		reqs := make([]*QueryReq, 0, 5)
		if err = common.UnmarshalJSON(b, &reqs); err != nil {
			return appErr.ErrUnableParseQuery.WithCause(err).WithCaller()
		}

		if len(reqs) == 0 {
			return nil
		}

		b := bytes.NewBuffer(nil)
		b.WriteByte(91)
		first := true
		n := 0
		for i := 0; i < len(reqs); i++ {
			if len(reqs[i].Sources) == 0 {
				continue
			}

			if !first {
				b.WriteByte(44)
			}
			b.Write(reqs[i].Sources)
			first = false
			n = n + 1
		}
		b.WriteByte(93)

		q.Function = reqs[0].Function
		q.Args = reqs[0].Args
		q.Sources = b.Bytes()
		q.Identity = reqs[0].Identity
		q.PermissionKey = reqs[0].PermissionKey
		q.NBatchSources = n

		if len(q.Sources) == 2 {
			q.Sources = nil
		}

		return nil
	} else if firstChar == firstCharObject {
		if err = common.UnmarshalJSON(b, q); err != nil {
			return appErr.ErrUnableParseQuery.WithCause(err).WithCaller()
		}

		return nil
	}

	return appErr.ErrUnableParseQuery.WithInput(string(b)).WithCaller()
}
