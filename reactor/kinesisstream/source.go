package kinesisstream

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor"
)

type source struct {
    recs *EventSource
    msgs event.Msgs
}

func (s *source) GetRequest(ctx context.Context, payload []byte) (*reactor.Request, errors.Error) {
    s.recs.Clear()
    if err := common.UnmarshalJSON(payload, s.recs); err != nil {
        return nil, err
    }

    req := &reactor.Request{}

    if s.recs.Warmer {
        req.Warmer = s.recs.Warmer
        req.Concurency = s.recs.Concurency

        return req, nil
    }

    s.msgs = s.msgs[:0]

    for _, rec := range s.recs.Records {
        s.msgs = append(s.msgs, rec.Kinesis.Data.EventMsg)
    }

    req.Msgs = s.msgs

    return req, nil
}

func New() reactor.EventSource {
    return &source{
        recs: &EventSource{
            Records: make(Records, 0, 100),
        },
        msgs: make(event.Msgs, 0, 100),
    }
}
