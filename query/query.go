package query

import (
	"bytes"
	"context"
	"encoding/json"

	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
)

type invokereq struct {
	Function      string           `json:"function"`
	Args          json.RawMessage  `json:"arguments"`
	Source        json.RawMessage  `json:"source,omitempty"`
	Identity      *invoke.Identity `json:"identity,omitempty"`
	PermissionKey string           `json:"pemKey,omitempty"`
	Warmer        bool             `json:"warmer,omitempty"`
	Concurency    int              `json:"concurency,omitempty"`
}

type Query struct {
	Function      string           `json:"function"`
	Args          json.RawMessage  `json:"arguments"`
	Sources       json.RawMessage  `json:"source,omitempty"`
	Identity      *invoke.Identity `json:"identity,omitempty"`
	NBatchSources int              `json:"-"`
	PermissionKey string           `json:"pemKey,omitempty"`
	Warmer        bool             `json:"warmer,omitempty"`
	Concurency    int              `json:"concurency,omitempty"`
}

func (e *Query) ParseArgs(v interface{}) errors.Error {
	if err := jsoniter.ConfigFastest.Unmarshal(e.Args, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *Query) ParseSource(v interface{}) errors.Error {
	if err := jsoniter.ConfigFastest.Unmarshal(e.Sources, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

type queryinfo struct {
	handler     QueryHandler
	prehandlers []QueryHandler
}

func WithPermission(pm string) QueryHandler {
	return func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		if query.Identity == nil {
			return nil, appErr.ErrPermissionDenied
		}

		if query.Identity.Claims.Permissions == nil {
			return nil, appErr.ErrPermissionDenied
		}

		if ok := query.Identity.Claims.Permissions.Has(query.PermissionKey, pm); !ok {
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

func (q *Query) UnmarshalJSON(b []byte) error {
	var err error
	firstChar := b[0]

	if firstChar == firstCharArray {
		reqs := make([]*invokereq, 0, 5)
		if err = jsoniter.ConfigFastest.Unmarshal(b, &reqs); err != nil {
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
			if len(reqs[i].Source) == 0 {
				continue
			}

			if !first {
				b.WriteByte(44)
			}
			b.Write(reqs[i].Source)
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
		req := &invokereq{}
		if err = jsoniter.ConfigFastest.Unmarshal(b, req); err != nil {
			return appErr.ErrUnableParseQuery.WithCause(err).WithCaller()
		}

		q.Function = req.Function
		q.Args = req.Args
		q.Sources = req.Source
		q.Identity = req.Identity
		q.PermissionKey = req.PermissionKey
		q.NBatchSources = 0
		q.Warmer = req.Warmer
		q.Concurency = req.Concurency

		return nil
	}

	return appErr.ErrUnableParseQuery.WithInput(string(b)).WithCaller()
}
