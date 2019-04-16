package random

import (
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor/dynamostream"
)

type dynamodbBuilder struct {
    event      *dynamostream.DynamoDBStreamEvent
    eventTypes common.Set
}

func DynamoDB() *dynamodbBuilder {
    return &dynamodbBuilder{
        event: &dynamostream.DynamoDBStreamEvent{
            Records: make([]*dynamostream.Record, 0, 100),
        },
        eventTypes: common.NewSet(),
    }
}

func (b *dynamodbBuilder) Build() *dynamostream.DynamoDBStreamEvent {
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

func (b *dynamodbBuilder) Add(events ...*eventstore.EventMsg) *dynamodbBuilder {
    for _, event := range events {
        b.event.Records = append(b.event.Records, &dynamostream.Record{
            EventName: dynamostream.EventInsert,
            DynamoDB: &dynamostream.DynamoDBRecord{
                NewImage: &dynamostream.Payload{
                    EventMsg: event,
                },
            },
        })
        b.eventTypes.Set(event.EventType)
    }

    return b
}
