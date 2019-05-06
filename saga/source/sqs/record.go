package sqs

import (
    "encoding/base64"

    "github.com/onedaycat/zamus/event"
)

type Source struct {
    EventMsg   []byte    `json:"eventMsg"`
    Resume     string    `json:"resume"`
    Records    []*Record `json:"Records"`
    Warmer     bool      `json:"warmer,omitempty"`
    Concurency int       `json:"concurency,omitempty"`
}

func (e *Source) Clear() {
    e.Records = e.Records[:0]
}

type Record struct {
    Body              *Payload          `json:"body"`
    MessageAttributes *MessageAttribute `json:"messageAttributes"`
}

type MessageAttribute struct {
    EventType *DataEventType `json:"event"`
}

type Payload struct {
    EventMsg *event.Msg
}

type DataEventType struct {
    Value string `json:"Value"`
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
