package random

import (
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor/source/kinesisstream"
)

type kinesisBuilder struct {
    event      *kinesisstream.Source
    eventTypes common.Set
}

func KinesisEvents() *kinesisBuilder {
    return &kinesisBuilder{
        event: &kinesisstream.Source{
            Records: make([]*kinesisstream.Record, 0, 100),
        },
        eventTypes: common.NewSet(),
    }
}

func (b *kinesisBuilder) Build() *kinesisstream.Source {
    return b.event
}

func (b *kinesisBuilder) BuildWithEventTypes() (*kinesisstream.Source, []string) {
    return b.event, b.eventTypes.List()
}

func (b *kinesisBuilder) BuildJSON() []byte {
    data, err := common.MarshalJSON(b.event)
    if err != nil {
        panic(err)
    }

    return data
}

func (b *kinesisBuilder) RandomMessage(n int) *kinesisBuilder {
    msgs := EventMsgs().RandomEventMsgs(n).Build()
    for i := 0; i < n; i++ {
        b.Add(msgs[i])
    }

    return b
}

func (b *kinesisBuilder) Add(events ...*event.Msg) *kinesisBuilder {
    for _, evt := range events {
        b.event.Records = append(b.event.Records, &kinesisstream.Record{
            Kinesis: &kinesisstream.KinesisPayload{
                PartitionKey: evt.AggID,
                Data: &kinesisstream.Payload{
                    EventMsg: evt,
                },
            },
        })
        b.eventTypes.Set(evt.EventType)
    }

    return b
}
