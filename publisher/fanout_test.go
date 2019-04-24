package publisher

import (
    "context"
    "testing"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/invoke"
    mockInovker "github.com/onedaycat/zamus/invoke/mocks"
    "github.com/onedaycat/zamus/random"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
)

func TestProcessInvokeSuccess(t *testing.T) {
    mockInvoke := &mockInovker.Invoker{}

    h := New(Config{
        Fanout: &FanoutConfig{
            Invoker: mockInvoke,
            Lambdas: []*LambdaConfig{
                {
                    Fn: "arn1",
                    Events: event.EventTypes(
                        (*domain.StockItemCreated)(nil),
                        (*domain.StockItemRemoved)(nil),
                    ),
                },
                {
                    Fn: "arn2",
                    Events: event.EventTypes(
                        (*domain.StockItemCreated)(nil),
                        (*domain.StockItemRemoved)(nil),
                        (*domain.StockItemUpdated)(nil),
                    ),
                },
            },
        },
    })

    msgs := random.EventMsgs().
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "1"})).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "2"})).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "3"})).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "4"})).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "5"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "6"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "7"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "8"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "9"})).
        Build()

    dyRecs := random.DynamoDB().Add(msgs...).BuildJSON()

    arn1Req := invoke.NewReactorRequest("arn1").WithEventList(&event.MsgList{
        Msgs: []*event.Msg{
            msgs[0], msgs[1], msgs[2], msgs[3], msgs[4],
        },
    })
    mockInvoke.On("InvokeReactor", mock.Anything, arn1Req).Return(nil).Once()

    arn2Req := invoke.NewReactorRequest("arn2").WithEventList(&event.MsgList{
        Msgs: msgs,
    })
    mockInvoke.On("InvokeReactor", mock.Anything, arn2Req).Return(nil).Once()

    res, err := h.Invoke(context.Background(), dyRecs)
    require.NoError(t, err)
    require.Nil(t, res)

    res, err = h.Invoke(context.Background(), dyRecs)
    require.NoError(t, err)
    require.Nil(t, res)
}

func TestProcessInvokeError(t *testing.T) {
    mockInvoke := &mockInovker.Invoker{}

    h := New(Config{
        Fanout: &FanoutConfig{
            Invoker: mockInvoke,
            Lambdas: []*LambdaConfig{
                {
                    Fn: "arn1",
                    Events: event.EventTypes(
                        (*domain.StockItemCreated)(nil),
                        (*domain.StockItemRemoved)(nil),
                    ),
                },
                {
                    Fn: "arn2",
                    Events: event.EventTypes(
                        (*domain.StockItemCreated)(nil),
                        (*domain.StockItemRemoved)(nil),
                        (*domain.StockItemUpdated)(nil),
                    ),
                },
            },
        },
    })

    msgs := random.EventMsgs().
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "1"})).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "2"})).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "3"})).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "4"})).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "5"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "6"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "7"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "8"})).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "9"})).
        Build()

    dyRecs := random.DynamoDB().Add(msgs...).Build()

    arn1Req := invoke.NewReactorRequest("arn1").WithEventList(&event.MsgList{
        Msgs: []*event.Msg{
            msgs[0], msgs[1], msgs[2], msgs[3], msgs[4],
        },
    })
    mockInvoke.On("InvokeReactor", mock.Anything, arn1Req).Return(errors.DumbError).Once()

    arn2Req := invoke.NewReactorRequest("arn2").WithEventList(&event.MsgList{
        Msgs: msgs,
    })
    mockInvoke.On("InvokeReactor", mock.Anything, arn2Req).Return(nil).Once()

    err := h.processInvoke(context.Background(), dyRecs)
    require.Equal(t, errors.DumbError, err)
}
