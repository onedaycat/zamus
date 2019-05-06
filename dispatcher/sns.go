package dispatcher

import (
    "context"

    "github.com/aws/aws-sdk-go/service/sns"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
)

var (
    snsStringDataType = "String"
    eventTypeKey      = "event"
)

type SNSConfig struct {
    TopicArn        string
    FilterEvents    []string
    Client          *sns.SNS
    FilterOutEvents bool

    records    event.Msgs
    eventTypes map[string]struct{}
    isAll      bool
    ctx        context.Context
}

func (c *SNSConfig) init() {
    if len(c.FilterEvents) > 0 {
        c.eventTypes = make(map[string]struct{})
        for _, eventType := range c.FilterEvents {
            c.eventTypes[eventType] = struct{}{}
        }
    } else {
        c.isAll = true
    }
    c.records = make(event.Msgs, 0, 100)
}

func (c *SNSConfig) filter(msg *event.Msg) {
    if c.isAll {
        c.records = append(c.records, msg)
    } else {
        if c.FilterOutEvents {
            _, ok := c.eventTypes[msg.EventType]
            if !ok {
                c.records = append(c.records, msg)
            }
        } else {
            _, ok := c.eventTypes[msg.EventType]
            if ok {
                c.records = append(c.records, msg)
            }
        }
    }
}

func (c *SNSConfig) clear() {
    c.records = c.records[:0]
}

func (c *SNSConfig) hasEvents() bool {
    return len(c.records) > 0 || c.isAll
}

func (c *SNSConfig) setContext(ctx context.Context) {
    c.ctx = ctx
}

func (c *SNSConfig) publish() errors.Error {
    for _, msg := range c.records {
        data := getBase64Msg(msg)
        _, err := c.Client.PublishWithContext(c.ctx, &sns.PublishInput{
            TopicArn: &c.TopicArn,
            Message:  &data,
            MessageAttributes: map[string]*sns.MessageAttributeValue{
                eventTypeKey: {
                    DataType:    &snsStringDataType,
                    StringValue: &msg.EventType,
                },
            },
        })
        if err != nil {
            Sentry(c.ctx, nil, ErrUnablePublishSNS.WithCaller().WithCause(err).WithInput(msg))
        }
    }

    return nil
}
