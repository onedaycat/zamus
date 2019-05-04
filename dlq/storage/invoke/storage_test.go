package invoke

import (
    "context"
    "testing"

    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/dlq/service"
    "github.com/onedaycat/zamus/invoke"
    "github.com/onedaycat/zamus/invoke/mocks"
    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
)

func TestCRUD(t *testing.T) {
    invoker := &mocks.Invoker{}
    s := New(invoker, "arn")

    msg := &dlq.DLQMsg{
        ID:         "123",
        Service:    "hello",
        Time:       111,
        Version:    "1.0.0",
        SourceType: dlq.Lambda,
        Source:     "sel-hello-dev-srv",
        EventMsgs:  []byte(`hello`),
        Errors:     nil,
    }

    input := &service.SaveDLQMsgInput{
        Msg: msg,
    }

    req := invoke.NewRequest(service.SaveDLQMsgMethod).WithInput(input)
    invoker.On("Invoke", mock.Anything, mock.Anything, req, nil).Return(nil)

    err := s.Save(context.Background(), &dlq.DLQMsg{
        ID:         "123",
        Service:    "hello",
        Time:       111,
        Version:    "1.0.0",
        SourceType: dlq.Lambda,
        Source:     "sel-hello-dev-srv",
        EventMsgs:  []byte(`hello`),
        Errors:     nil,
    })

    require.NoError(t, err)
}
