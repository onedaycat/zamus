package random

import (
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/eventstore"
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

func (k *dynamodbBuilder) Build() *dynamostream.DynamoDBStreamEvent {
	return k.event
}

func (b *dynamodbBuilder) BuildJSON() []byte {
	data, err := common.MarshalJSON(b.event)
	if err != nil {
		panic(err)
	}

	return data
}

func (k *dynamodbBuilder) RandomMessage(n int) *dynamodbBuilder {
	msgs := EventMsgs().RandomEventMsgs(n).Build()
	for i := 0; i < n; i++ {
		k.Add(msgs[i])
	}

	return k
}

func (k *dynamodbBuilder) Add(events ...*eventstore.EventMsg) *dynamodbBuilder {
	for _, event := range events {
		k.event.Records = append(k.event.Records, &dynamostream.Record{
			EventName: dynamostream.EventInsert,
			DynamoDB: &dynamostream.DynamoDBRecord{
				NewImage: &dynamostream.Payload{
					EventMsg: event,
				},
			},
		})
		k.eventTypes.Set(event.EventType)
	}

	return k
}
