package query

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/invoke"
)

type Query struct {
	Function      string           `json:"function"`
	Args          json.RawMessage  `json:"arguments"`
	Sources       json.RawMessage  `json:"sources"`
	Identity      *invoke.Identity `json:"identity"`
	NBatchSources int              `json:"-"`
	PermissionKey string           `json:"pemKey"`
}

type queryinfo struct {
	handler     QueryHandler
	prehandlers []QueryHandler
}

func WithPermission(pm string) QueryHandler {
	return func(ctx context.Context, query *Query) (QueryResult, error) {
		if query.Identity == nil {
			return nil, ErrPermissionDenied
		}

		if query.Identity.Claims.Permissions == nil {
			return nil, ErrPermissionDenied
		}

		if ok := query.Identity.Claims.Permissions.Has(query.PermissionKey, pm); !ok {
			return nil, ErrPermissionDenied
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
		invokes := make([]*invoke.InvokeEvent, 0, 5)
		if err = json.Unmarshal(b, &invokes); err != nil {
			return ErrUnableParseQuery.WithCause(err).WithCaller()
		}

		if len(invokes) == 0 {
			return nil
		}

		b := bytes.NewBuffer(nil)
		b.WriteByte(91)
		first := true
		n := 0
		for i := 0; i < len(invokes); i++ {
			if len(invokes[i].Source) == 0 {
				continue
			}

			if !first {
				b.WriteByte(44)
			}
			b.Write(invokes[i].Source)
			first = false
			n = n + 1
		}
		b.WriteByte(93)

		q.Function = invokes[0].Function
		q.Args = invokes[0].Args
		q.Sources = b.Bytes()
		q.Identity = invokes[0].Identity
		q.PermissionKey = invokes[0].PermissionKey
		q.NBatchSources = n

		if len(q.Sources) == 2 {
			q.Sources = nil
		}

		return nil
	} else if firstChar == firstCharObject {
		invoke := &invoke.InvokeEvent{}
		if err = json.Unmarshal(b, invoke); err != nil {
			return ErrUnableParseQuery.WithCause(err).WithCaller()
		}

		q.Function = invoke.Function
		q.Args = invoke.Args
		q.Sources = invoke.Source
		q.Identity = invoke.Identity
		q.PermissionKey = invoke.PermissionKey
		q.NBatchSources = 0

		return nil
	}

	return errors.InternalError("QUERY_JSON_PARSE", "Unable to parse request").WithInput(errors.Input{"input": string(b)}).WithCaller()
}
