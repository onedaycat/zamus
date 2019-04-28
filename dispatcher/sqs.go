package dispatcher

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
)

var (
	sqsStringDataType = "String"
	sqsBinaryDataType = "Binary"
)

type SQSConfig struct {
	QueueUrl     string
	FilterEvents []string

	records    []*sqs.SendMessageBatchRequestEntry
	eventTypes map[string]struct{}
	isAll      bool
	client     *sqs.SQS
	ctx        context.Context
}

func (c *SQSConfig) init() {
	if len(c.FilterEvents) > 0 {
		c.eventTypes = make(map[string]struct{})
		for _, eventType := range c.FilterEvents {
			c.eventTypes[eventType] = struct{}{}
		}
	} else {
		c.isAll = true
	}
	c.records = make([]*sqs.SendMessageBatchRequestEntry, 0, 100)
}

func (c *SQSConfig) filter(msg *event.Msg) {
	if c.isAll {
		data, _ := event.MarshalMsg(msg)
		c.records = append(c.records, &sqs.SendMessageBatchRequestEntry{
			Id:          &msg.Id,
			MessageBody: &msg.Id,
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				msgKey: {
					BinaryValue: data,
					DataType:    &sqsBinaryDataType,
				},
				eventTypeKey: {
					StringValue: &msg.EventType,
					DataType:    &sqsStringDataType,
				},
			},
		})
	} else {
		_, ok := c.eventTypes[msg.EventType]
		if ok {
			data, _ := event.MarshalMsg(msg)
			c.records = append(c.records, &sqs.SendMessageBatchRequestEntry{
				Id:          &msg.Id,
				MessageBody: &msg.Id,
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
					msgKey: {
						BinaryValue: data,
						DataType:    &sqsBinaryDataType,
					},
					eventTypeKey: {
						StringValue: &msg.EventType,
						DataType:    &sqsStringDataType,
					},
				},
			})
		}
	}
}

func (c *SQSConfig) clear() {
	c.records = c.records[:0]
}

func (c *SQSConfig) hasEvents() bool {
	return len(c.records) > 0 || c.isAll
}

func (c *SQSConfig) setContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *SQSConfig) publish() errors.Error {
	_, err := c.client.SendMessageBatchWithContext(c.ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &c.QueueUrl,
		Entries:  c.records,
	})
	if err != nil {
		Sentry(c.ctx, nil, ErrUnablePublishSQS.WithCaller().WithCause(err).WithInput(c.records))
	}

	return nil
}
