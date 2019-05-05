package invoke

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/dlq/service"
    "github.com/onedaycat/zamus/invoke"
)

var (
    ErrUnableSaveDLQ = errors.DefInternalError("ErrUnableSaveDLQ", "Unable to save DLQ")
)

type invokeService struct {
    invoker invoke.Invoker
    fn      string
}

func New(invoker invoke.Invoker, fn string) *invokeService {
    return &invokeService{
        invoker: invoker,
        fn:      fn,
    }
}

func (s *invokeService) Save(ctx context.Context, dlqMsg *dlq.DLQMsg) errors.Error {
    input := &service.SaveDLQMsgInput{
        Msg: dlqMsg,
    }

    req := invoke.NewRequest(service.SaveDLQMsgMethod).WithInput(input)
    if err := s.invoker.Invoke(ctx, s.fn, req, nil); err != nil {
        return ErrUnableSaveDLQ.WithCause(err).WithCaller().WithInput(input)
    }

    return nil
}

func (s *invokeService) Get(ctx context.Context, lambdaType dlq.LambdaType, id string) (*dlq.DLQMsg, errors.Error) {
    return nil, nil
}
