package random

import (
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor/source/sqs"
)

type sqsBuilder struct {
    event      *sqs.Source
    eventTypes common.Set
}

func SQSEvents() *sqsBuilder {
    return &sqsBuilder{
        event: &sqs.Source{
            Records: make([]*sqs.Record, 0, 100),
        },
        eventTypes: common.NewSet(),
    }
}

func (b *sqsBuilder) Build() *sqs.Source {
    return b.event
}

func (b *sqsBuilder) BuildWithEventTypes() (*sqs.Source, []string) {
    return b.event, b.eventTypes.List()
}

func (b *sqsBuilder) BuildJSON() []byte {
    data, err := common.MarshalJSON(b.event)
    if err != nil {
        panic(err)
    }

    return data
}

func (b *sqsBuilder) RandomMessage(n int) *sqsBuilder {
    msgs := EventMsgs().RandomEventMsgs(n).Build()
    for i := 0; i < n; i++ {
        b.Add(msgs[i])
    }

    return b
}

func (b *sqsBuilder) Add(events ...*event.Msg) *sqsBuilder {
    for _, evt := range events {
        b.event.Records = append(b.event.Records, &sqs.Record{
            Body: &sqs.Payload{
                EventMsg: evt,
            },
            MessageAttributes: &sqs.MessageAttribute{
                EventType: &sqs.DataEventType{
                    Value: evt.EventType,
                },
            },
        })
        b.eventTypes.Set(evt.EventType)
    }

    return b
}
