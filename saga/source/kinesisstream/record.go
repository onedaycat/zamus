package kinesisstream

import (
    "encoding/base64"

    "github.com/onedaycat/zamus/event"
)

type Records = []*Record

type Source struct {
    EventMsg   []byte  `json:"eventMsg"`
    Resume     string  `json:"resume"`
    Records    Records `json:"Records"`
    Warmer     bool    `json:"warmer,omitempty"`
    Concurency int     `json:"concurency,omitempty"`
}

func (e *Source) Clear() {
    e.Records = e.Records[:0]
    e.Warmer = false
    e.Concurency = 0
}

type Record struct {
    Kinesis *KinesisPayload `json:"kinesis"`
}

type KinesisPayload struct {
    PartitionKey string   `json:"partitionKey"`
    Data         *Payload `json:"data"`
}

type Payload struct {
    EventMsg *event.Msg
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

    p.EventMsg = &event.Msg{}
    if err = event.UnmarshalMsg(bdata[:n], p.EventMsg); err != nil {
        return err
    }

    return nil
}
