package dispatcher

import (
    "context"
    "testing"

    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/invoke"
    "github.com/onedaycat/zamus/invoke/mocks"
    "github.com/onedaycat/zamus/random"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestSagaFilterAndPublish(t *testing.T) {
    config := &SagaConfig{
        Fn:          "arn1",
        FilterEvent: event.EventType((*domain.StockItemCreated)(nil)),
        Input: func(msg *event.Msg) interface{} {
            evt := &domain.StockItemCreated{}
            msg.MustUnmarshalEvent(evt)

            return evt.ProductID
        },
    }
    config.init()

    msgs := random.EventMsgs().
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "1"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "2"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "3"}), random.WithAggregateID("2")).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "4"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "5"}), random.WithAggregateID("2")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "6"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "7"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "8"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "9"}), random.WithAggregateID("2")).
        Build()

    for _, msg := range msgs {
        config.filter(msg)
    }

    require.Len(t, config.records, 2)
    require.Equal(t, event.Msgs{
        msgs[0], msgs[1],
    }, config.records["1"])

    require.Equal(t, event.Msgs{
        msgs[2],
    }, config.records["2"])

    mockClient := &mocks.Invoker{}
    config.Client = mockClient
    config.setContext(context.Background())

    mockClient.On("InvokeSaga", config.ctx,
        invoke.NewSagaRequest(config.Fn).WithInput("1"),
        nil).Return(nil).Once()
    mockClient.On("InvokeSaga", config.ctx,
        invoke.NewSagaRequest(config.Fn).WithInput("2"),
        nil).Return(nil).Once()
    mockClient.On("InvokeSaga", config.ctx,
        invoke.NewSagaRequest(config.Fn).WithInput("3"),
        nil).Return(nil).Once()
    err := config.publish()
    require.NoError(t, err)
    mockClient.AssertExpectations(t)

    mockClient.On("InvokeSaga", config.ctx,
        invoke.NewSagaRequest(config.Fn).WithInput("1"),
        nil).Return(nil).Once()
    mockClient.On("InvokeSaga", config.ctx,
        invoke.NewSagaRequest(config.Fn).WithInput("2"),
        nil).Return(nil).Once()
    mockClient.On("InvokeSaga", config.ctx,
        invoke.NewSagaRequest(config.Fn).WithInput("3"),
        nil).Return(nil).Once()

    err = config.publish()
    require.NoError(t, err)
    mockClient.AssertExpectations(t)
}

func TestSagaClearAndHasEvent(t *testing.T) {
    config := &SagaConfig{
        Fn:          "arn1",
        FilterEvent: event.EventType((*domain.StockItemCreated)(nil)),
    }
    config.init()

    msgs := random.EventMsgs().
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "1"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "2"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemCreated{ProductID: "3"}), random.WithAggregateID("2")).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "4"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemRemoved{ProductID: "5"}), random.WithAggregateID("2")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "6"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "7"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "8"}), random.WithAggregateID("1")).
        Add(random.WithEvent(&domain.StockItemUpdated{ProductID: "9"}), random.WithAggregateID("2")).
        Build()

    for _, msg := range msgs {
        config.filter(msg)
    }

    require.True(t, config.hasEvents())
    config.clear()
    require.False(t, config.hasEvents())
}
