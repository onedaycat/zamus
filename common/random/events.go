package random

import (
    "github.com/gogo/protobuf/proto"
    "github.com/onedaycat/zamus/common"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/reactor/kinesisstream"
)

type EventMsgsOption func(opts *eventMsgsOptions)

type eventMsgsOptions struct {
    meatadata eventstore.Metadata
    aggid     string
    eventTime int64
    version   string
    events    []proto.Message
}

func WithMetadata(meatadata eventstore.Metadata) EventMsgsOption {
    return func(opts *eventMsgsOptions) {
        opts.meatadata = meatadata
    }
}

func WithAggregateID(aggid string) EventMsgsOption {
    return func(opts *eventMsgsOptions) {
        opts.aggid = aggid
    }
}

func WithTime(t int64) EventMsgsOption {
    return func(opts *eventMsgsOptions) {
        opts.eventTime = t
    }
}

func WithEvent(events ...proto.Message) EventMsgsOption {
    return func(opts *eventMsgsOptions) {
        opts.events = events
    }
}

func WithVersion(version string) EventMsgsOption {
    return func(opts *eventMsgsOptions) {
        opts.version = version
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

func (b *eventsBuilder) Add(options ...EventMsgsOption) *eventsBuilder {
    opts := &eventMsgsOptions{}
    for _, opt := range options {
        opt(opts)
    }

    msgBuilder := EventMsg().Seq(b.seq)

    b.setOptions(opts, msgBuilder, 0)

    b.msgs = append(b.msgs, msgBuilder.Build())
    b.seq++

    return b
}

func (b *eventsBuilder) AddEventMsgs(msgs ...*eventstore.EventMsg) *eventsBuilder {
    for _, msg := range msgs {
        b.msgs = append(b.msgs, msg)
        b.seq++
    }

    return b
}

func (b *eventsBuilder) RandomEventMsgs(n int, options ...EventMsgsOption) *eventsBuilder {
    opts := &eventMsgsOptions{}
    for _, opt := range options {
        opt(opts)
    }

    for i := 0; i < n; i++ {
        msg := EventMsg()
        b.setOptions(opts, msg, i)
        b.msgs = append(b.msgs, msg.Build())
        b.seq++
    }

    return b
}

func (b *eventsBuilder) BuildJSON() []byte {
    data, err := common.MarshalJSON(b.msgs)
    if err != nil {
        panic(err)
    }

    return data
}

func (b *eventsBuilder) BuildKinesis() *kinesisstream.KinesisStreamEvent {
    return KinesisEvents().Add(b.msgs...).Build()
}

func (b *eventsBuilder) BuildKinesisWithEventTypes() (*kinesisstream.KinesisStreamEvent, []string) {
    return KinesisEvents().Add(b.msgs...).BuildWithEventTypes()
}

func (b *eventsBuilder) setOptions(opts *eventMsgsOptions, builder *eventBuilder, index int) {
    if opts.meatadata != nil {
        builder.Metadata(opts.meatadata)
    }

    if opts.aggid != "" {
        builder.AggregateID(opts.aggid)
    }

    if opts.eventTime > 0 {
        builder.Time(opts.eventTime)
    }

    if opts.version != "" {
        builder.Versionn(opts.version)
    }

    if len(opts.events) > 0 {
        builder.Event(opts.events[0])
    }
}
