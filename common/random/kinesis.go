package random

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
)

type kinesisBuilder struct {
	event *kinesisstream.KinesisStreamEvent
}

func KinesisEvents() *kinesisBuilder {
	return &kinesisBuilder{
		event: &kinesisstream.KinesisStreamEvent{
			Records: make([]*kinesisstream.Record, 0, 100),
		},
	}
}

func (k *kinesisBuilder) Build() *kinesisstream.KinesisStreamEvent {
	return k.event
}

func (b *kinesisBuilder) BuildJSON() []byte {
	data, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(b.event)
	if err != nil {
		panic(err)
	}

	return data
}

func (k *kinesisBuilder) Add(partitionKey string, events ...*eventstore.EventMsg) *kinesisBuilder {
	for _, event := range events {
		k.event.Records = append(k.event.Records, &kinesisstream.Record{
			Kinesis: &kinesisstream.KinesisPayload{
				PartitionKey: partitionKey,
				Data: &kinesisstream.Payload{
					EventMsg: event,
				},
			},
		})
	}

	return k
}
