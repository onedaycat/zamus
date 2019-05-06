package sqs

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor"
)

type source struct {
    recs *Source
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

    if s.recs.Msgs != nil {
        msgList := &event.MsgList{}
        if err := event.UnmarshalMsg(s.recs.Msgs, msgList); err != nil {
            return nil, err
        }

        for _, msg := range msgList.Msgs {
            s.msgs = append(s.msgs, msg)
        }
    }

    for _, rec := range s.recs.Records {
        s.msgs = append(s.msgs, rec.Body.EventMsg)
    }

    req.Msgs = s.msgs

    return req, nil
}

func New() reactor.Source {
    return &source{
        recs: &Source{
            Records: make([]*Record, 0, 100),
        },
        msgs: make(event.Msgs, 0, 100),
    }
}
