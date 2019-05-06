package random

import (
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor/source/sns"
)

type snsBuilder struct {
    event      *sns.Source
    eventTypes common.Set
}

func SNSEvent() *snsBuilder {
    return &snsBuilder{
        event: &sns.Source{
            Records: make([]*sns.Record, 0, 1),
        },
        eventTypes: common.NewSet(),
    }
}

func (b *snsBuilder) Build() *sns.Source {
    return b.event
}

func (b *snsBuilder) BuildWithEventTypes() (*sns.Source, []string) {
    return b.event, b.eventTypes.List()
}

func (b *snsBuilder) BuildJSON() []byte {
    data, err := common.MarshalJSON(b.event)
    if err != nil {
        panic(err)
    }

    return data
}

func (b *snsBuilder) RandomMessage() *snsBuilder {
    msgs := EventMsgs().RandomEventMsgs(1).Build()
    for i := 0; i < 1; i++ {
        b.Add(msgs[i])
    }

    return b
}

func (b *snsBuilder) Add(msg *event.Msg) *snsBuilder {
    b.event.Records = append(b.event.Records, &sns.Record{
        SNS: &sns.SNS{
            Message: &sns.Payload{EventMsg: msg},
            MessageAttributes: &sns.MessageAttribute{
                EventType: &sns.DataEventType{
                    Value: msg.EventType,
                },
            },
        },
    })
    b.eventTypes.Set(msg.EventType)

    return b
}
