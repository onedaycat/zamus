package kinesisstream

import (
	"encoding/base64"

	"github.com/onedaycat/zamus/eventstore"
)

type Records = []*Record

type KinesisStreamEvent struct {
	Records Records `json:"Records"`
}

type Record struct {
	EventID   string          `json:"eventID"`
	EventName string          `json:"eventName"`
	Kinesis   *KinesisPayload `json:"kinesis"`
}

func (r *Record) add(key, eid string) {
	r.Kinesis = &KinesisPayload{
		PartitionKey: key,
		Data: &Payload{
			EventMessage: &EventMessage{
				EventID: eid,
			},
		},
	}
}

type KinesisPayload struct {
	PartitionKey string   `json:"partitionKey"`
	Data         *Payload `json:"data"`
}

type Payload struct {
	EventMessage *eventstore.EventMsg
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

	p.EventMessage = &eventstore.EventMsg{}
	if err = p.EventMessage.Unmarshal(bdata[:n]); err != nil {
		return err
	}

	return nil
}
