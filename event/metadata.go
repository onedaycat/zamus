package event

import (
	"context"
)

type evtMetaKey struct{}

var evtMetacontextKey = &evtMetaKey{}

type Metadata = map[string]string

func NewMetadataContext(parent context.Context, meta Metadata) context.Context {
	return context.WithValue(parent, evtMetacontextKey, meta)
}

func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	meta, ok := ctx.Value(evtMetacontextKey).(Metadata)
	return meta, ok
}
