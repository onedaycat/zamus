package random

import (
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor/dynamostream"
)

type dynamodbBuilder struct {
    event      *dynamostream.EventSource
    eventTypes common.Set
}

func DynamoDB() *dynamodbBuilder {
    return &dynamodbBuilder{
        event: &dynamostream.EventSource{
            Records: make([]*dynamostream.Record, 0, 100),
        },
        eventTypes: common.NewSet(),
    }
}

func (b *dynamodbBuilder) Build() *dynamostream.EventSource {
    return b.event
}

func (b *dynamodbBuilder) BuildJSON() []byte {
    data, err := common.MarshalJSON(b.event)
    if err != nil {
        panic(err)
    }

    return data
}

func (b *dynamodbBuilder) RandomMessage(n int) *dynamodbBuilder {
    msgs := EventMsgs().RandomEventMsgs(n).Build()
    for i := 0; i < n; i++ {
        b.Add(msgs[i])
    }

    return b
}

func (b *dynamodbBuilder) Add(events ...*event.Msg) *dynamodbBuilder {
    for _, evt := range events {
        b.event.Records = append(b.event.Records, &dynamostream.Record{
            EventName: dynamostream.EventInsert,
            DynamoDB: &dynamostream.DynamoDBRecord{
                NewImage: &dynamostream.Payload{
                    EventMsg: evt,
                },
            },
        })
        b.eventTypes.Set(evt.EventType)
    }

    return b
}
