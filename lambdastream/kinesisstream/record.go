package kinesisstream

import (
	"encoding/base64"

	"github.com/onedaycat/zamus/eventstore"
)

type Records = []*Record

type KinesisStreamEvent struct {
	Records    Records `json:"Records"`
	Warmer     bool    `json:"warmer,omitempty"`
	Concurency int     `json:"concurency,omitempty"`
}

type Record struct {
	Kinesis *KinesisPayload `json:"kinesis"`
}

func (r *Record) Add(pk, eid, etype string) {
	r.Kinesis = &KinesisPayload{
		PartitionKey: pk,
		Data: &Payload{
			EventMsg: &EventMsg{
				AggregateID: pk,
				EventID:     eid,
				EventType:   etype,
			},
		},
	}
}

type KinesisPayload struct {
	PartitionKey string   `json:"partitionKey"`
	Data         *Payload `json:"data"`
}

type Payload struct {
	EventMsg *eventstore.EventMsg
}

func (p *Payload) UnmarshalJSON(b []byte) error {
	var err error
	var bdata []byte

	b = b[1 : len(b)-1]
	bdata = make([]byte, base64.StdEncoding.DecodedLen(len(b)))

	n, err := base64.StdEncoding.Decode(bdata, b)
	if err != nil {
		return err
	}

	p.EventMsg = &eventstore.EventMsg{}
	if err = p.EventMsg.Unmarshal(bdata[:n]); err != nil {
		return err
	}

	return nil
}
