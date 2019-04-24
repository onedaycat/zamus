package invoke

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/reactor"
)

type source struct {
    recs *event.MsgList
}

func (s *source) GetRequest(ctx context.Context, payload []byte) (*reactor.Request, errors.Error) {
    s.clear()
    if err := event.UnmarshalMsg(payload, s.recs); err != nil {
        return nil, err
    }

    req := &reactor.Request{}

    req.Msgs = s.recs.Msgs

    return req, nil
}

func (s *source) clear() {
    s.recs.Msgs = s.recs.Msgs[:0]
}

func New() reactor.EventSource {
    return &source{
        recs: &event.MsgList{
            Msgs: make(event.Msgs, 0, 100),
        },
    }
}
