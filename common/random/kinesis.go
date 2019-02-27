package random

import (
	"strconv"

	"github.com/Pallinder/go-randomdata"
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

func (k *kinesisBuilder) AddEvents(nPartitionKey int, events ...*eventstore.EventMsg) *kinesisBuilder {
	pks := make([]string, nPartitionKey)
	for i := 0; i < nPartitionKey; i++ {
		pks[i] = "pk" + strconv.Itoa(i)
	}

	for _, event := range events {
		k.event.Records = append(k.event.Records, &kinesisstream.Record{
			Kinesis: &kinesisstream.KinesisPayload{
				PartitionKey: randomdata.StringSample(pks...),
				Data: &kinesisstream.Payload{
					EventMsg: event,
				},
			},
		})
	}

	return nil
}
