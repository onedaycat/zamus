package random

import (
	"encoding/json"

	"github.com/onedaycat/zamus/eventstore"
)

type EventMsgOption func(opts *eventMsgOptions)

type eventMsgOptions struct {
	meatadata *eventstore.Metadata
	aggid     string
	eventTime int64
}

func WithMetadata(meatadata *eventstore.Metadata) EventMsgOption {
	return func(opts *eventMsgOptions) {
		opts.meatadata = meatadata
	}
}

func WithAggregateID(aggid string) EventMsgOption {
	return func(opts *eventMsgOptions) {
		opts.aggid = aggid
	}
}

func WithTime(t int64) EventMsgOption {
	return func(opts *eventMsgOptions) {
		opts.eventTime = t
	}
}

type eventsBuilder struct {
	seq  int64
	msgs []*eventstore.EventMsg
}

func EventMsgs() *eventsBuilder {
	return &eventsBuilder{
		msgs: make([]*eventstore.EventMsg, 0, 100),
		seq:  1,
	}
}

func (b *eventsBuilder) Build() []*eventstore.EventMsg {
	return b.msgs
}

func (b *eventsBuilder) Add(eventType string, event interface{}, options ...EventMsgOption) *eventsBuilder {
	msgBuilder := EventMsg().Event(eventType, event).Seq(b.seq)

	opts := &eventMsgOptions{}
	for _, opt := range options {
		opt(opts)
	}

	if opts.meatadata != nil {
		msgBuilder.Metadata(opts.meatadata)
	}

	if opts.aggid != "" {
		msgBuilder.AggregateID(opts.aggid)
	}

	if opts.eventTime > 0 {
		msgBuilder.Time(opts.eventTime)
	}

	b.msgs = append(b.msgs, msgBuilder.Build())
	b.seq++

	return b
}

func (b *eventsBuilder) BuildJSON() []byte {
	data, err := json.Marshal(b.msgs)
	if err != nil {
		panic(err)
	}

	return data
}
