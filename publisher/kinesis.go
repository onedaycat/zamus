package publisher

import (
	"context"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
)

type KinesisConfig struct {
	StreamARN    string
	FilterEvents []string
	records      []*kinesis.PutRecordsRequestEntry
	eventTypes   map[string]struct{}
	isAll        bool
	client       KinesisPublisher
	ctx          context.Context
}

func (c *KinesisConfig) init() {
	if len(c.FilterEvents) > 0 {
		c.eventTypes = make(map[string]struct{})
		for _, eventType := range c.FilterEvents {
			c.eventTypes[eventType] = struct{}{}
		}
	} else {
		c.isAll = true
	}
	c.records = make([]*kinesis.PutRecordsRequestEntry, 0, 100)
}

func (c *KinesisConfig) filter(msg *event.Msg) {
	if c.isAll {
		data, _ := event.MarshalMsg(msg)
		c.records = append(c.records, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &msg.AggID,
		})
	} else {
		_, ok := c.eventTypes[msg.EventType]
		if ok {
			data, _ := event.MarshalMsg(msg)
			c.records = append(c.records, &kinesis.PutRecordsRequestEntry{
				Data:         data,
				PartitionKey: &msg.AggID,
			})
		}
	}
}

func (c *KinesisConfig) clear() {
	c.records = c.records[:0]
}

func (c *KinesisConfig) hasEvents() bool {
	return len(c.records) > 0 || c.isAll
}

func (c *KinesisConfig) publish() errors.Error {
	input := &kinesis.PutRecordsInput{
		Records:    c.records,
		StreamName: &c.StreamARN,
	}

	out, err := c.client.PutRecordsWithContext(c.ctx, input)
	if err != nil {
		return ErrUnablePublishKinesis.WithCaller().WithCause(err)
	}

	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		return ErrUnablePublishKinesis.WithCaller().WithCause(errors.Simple("One or more events published failed"))
	}

	return nil
}

func (c *KinesisConfig) setContext(ctx context.Context) {
	c.ctx = ctx
}
