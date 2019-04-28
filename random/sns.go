package random

import (
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/reactor/sns"
)

type snsBuilder struct {
	event      *sns.EventSource
	eventTypes common.Set
}

func SNSEvent() *snsBuilder {
	return &snsBuilder{
		event: &sns.EventSource{
			Records: make([]*sns.Record, 0, 1),
		},
		eventTypes: common.NewSet(),
	}
}

func (b *snsBuilder) Build() *sns.EventSource {
	return b.event
}

func (b *snsBuilder) BuildWithEventTypes() (*sns.EventSource, []string) {
	return b.event, b.eventTypes.List()
}

func (b *snsBuilder) BuildJSON() []byte {
	data, err := common.MarshalJSON(b.event)
	if err != nil {
		panic(err)
	}

	return data
}

func (b *snsBuilder) RandomMessage() *snsBuilder {
	msgs := EventMsgs().RandomEventMsgs(1).Build()
	for i := 0; i < 1; i++ {
		b.Add(msgs[i])
	}

	return b
}

func (b *snsBuilder) Add(evt *event.Msg) *snsBuilder {
	b.event.Records = append(b.event.Records, &sns.Record{
		SNS: &sns.SNS{
			MessageAttributes: &sns.MessageAttribute{
				Msg: &sns.DataMsg{
					Value: &sns.Payload{
						EventMsg: evt,
					},
				},
				EventType: &sns.DataEventType{
					Value: evt.EventType,
				},
			},
		},
	})
	b.eventTypes.Set(evt.EventType)

	return b
}
