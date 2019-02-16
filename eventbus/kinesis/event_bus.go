package kinesis

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/gocqrs"
)

var (
	ErrPublishFailed = errors.New("One or more events published failed")
)

type KinesisEventBus struct {
	kin        *kinesis.Kinesis
	streamName string
}

func NewKinesisEventBus(sess *session.Session, streamName string) *KinesisEventBus {
	return &KinesisEventBus{
		kin:        kinesis.New(sess),
		streamName: streamName,
	}
}

func (k *KinesisEventBus) Publish(events []*gocqrs.EventMessage) error {
	records := make([]*kinesis.PutRecordsRequestEntry, len(events))
	wg := sync.WaitGroup{}
	wg.Add(len(events))

	for i := 0; i < len(events); i++ {
		go func(index int, event *gocqrs.EventMessage) {
			data, _ := json.Marshal(event)
			records[index] = &kinesis.PutRecordsRequestEntry{
				Data:         data,
				PartitionKey: &event.PartitionKey,
			}
			wg.Done()
		}(i, events[i])
	}
	wg.Wait()

	out, err := k.kin.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
		StreamName: &k.streamName,
	})

	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		return ErrPublishFailed
	}

	return err
}
