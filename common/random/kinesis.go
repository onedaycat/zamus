package random

import (
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
)

type kinesisBuilder struct {
	event      *kinesisstream.KinesisStreamEvent
	eventTypes common.Set
}

func KinesisEvents() *kinesisBuilder {
	return &kinesisBuilder{
		event: &kinesisstream.KinesisStreamEvent{
			Records: make([]*kinesisstream.Record, 0, 100),
		},
		eventTypes: common.NewSet(),
	}
}

func (k *kinesisBuilder) Build() *kinesisstream.KinesisStreamEvent {
	return k.event
}

func (k *kinesisBuilder) BuildWithEventTypes() (*kinesisstream.KinesisStreamEvent, []string) {
	return k.event, k.eventTypes.List()
}

func (b *kinesisBuilder) BuildJSON() []byte {
	data, err := common.MarshalJSON(b.event)
	if err != nil {
		panic(err)
	}

	return data
}

func (k *kinesisBuilder) RandomMessage(n int) *kinesisBuilder {
	msgs := EventMsgs().RandomEventMsgs(n).Build()
	for i := 0; i < n; i++ {
		k.Add(msgs[i])
	}

	return k
}

func (k *kinesisBuilder) Add(events ...*eventstore.EventMsg) *kinesisBuilder {
	for _, event := range events {
		k.event.Records = append(k.event.Records, &kinesisstream.Record{
			Kinesis: &kinesisstream.KinesisPayload{
				PartitionKey: event.AggregateID,
				Data: &kinesisstream.Payload{
					EventMsg: event,
				},
			},
		})
		k.eventTypes.Set(event.EventType)
	}

	return k
}
