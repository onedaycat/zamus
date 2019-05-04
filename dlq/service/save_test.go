package service

import (
    "context"
    "testing"

    "github.com/onedaycat/zamus/dlq"
    "github.com/onedaycat/zamus/dlq/mocks"
    "github.com/onedaycat/zamus/service"
    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
)

func TestSaveSuccess(t *testing.T) {
    storage := &mocks.Storage{}

    h := NewHandler(storage)

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

    input := &SaveDLQMsgInput{
        Msg: msg,
    }

    req := service.NewRequest(SaveDLQMsgMethod).WithInput(input)
    storage.On("Save", mock.Anything, msg).Return(nil)

    res, err := h.SaveDLQMsg(context.Background(), req)
    require.NoError(t, err)
    require.Equal(t, "123", res)
}
