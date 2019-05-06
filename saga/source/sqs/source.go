package sqs

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/saga"
)

type source struct {
    recs *Source
    reqs []*saga.Request
}

func (s *source) GetRequest(ctx context.Context, payload []byte) ([]*saga.Request, errors.Error) {
    s.recs.Clear()
    if err := common.UnmarshalJSON(payload, s.recs); err != nil {
        return nil, err
    }

    s.reqs = s.reqs[:0]

    if s.recs.Resume != "" {
        s.reqs = append(s.reqs, &saga.Request{
            Resume: s.recs.Resume,
        })

        return s.reqs, nil
    }

    if s.recs.EventMsg != nil {
        msg := &event.Msg{}
        if err := event.UnmarshalMsg(s.recs.EventMsg, msg); err != nil {
            return nil, err
        }

        s.reqs = append(s.reqs, &saga.Request{
            EventMsg: msg,
        })

        return s.reqs, nil
    }

    for _, rec := range s.recs.Records {
        s.reqs = append(s.reqs, &saga.Request{
            EventMsg: rec.Body.EventMsg,
        })
    }

    return s.reqs, nil
}

func New() saga.Source {
    return &source{
        recs: &Source{
            Records: make([]*Record, 0, 100),
        },
        reqs: make([]*saga.Request, 0, 100),
    }
}
