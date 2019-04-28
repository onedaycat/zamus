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
		Fn: "arn1",
		FilterEvents: event.EventTypes(
			(*domain.StockItemCreated)(nil),
			(*domain.StockItemRemoved)(nil),
		),
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
		msgs[0], msgs[1], msgs[3],
	}, config.records["1"])

	require.Equal(t, event.Msgs{
		msgs[2], msgs[4],
	}, config.records["2"])

	mockClient := &mocks.Invoker{}
	config.client = mockClient
	config.setContext(context.Background())

	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["1"][0]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["1"][1]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["1"][2]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["2"][0]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["2"][1]),
		nil).Return(nil).Once()
	err := config.publish()
	require.NoError(t, err)
	mockClient.AssertExpectations(t)

	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["1"][0]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["1"][1]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["1"][2]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["2"][0]),
		nil).Return(nil).Once()
	mockClient.On("InvokeSaga", config.ctx,
		invoke.NewSagaRequest(config.Fn).WithInput(config.records["2"][1]),
		nil).Return(nil).Once()

	err = config.publish()
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestSagaAllEvents(t *testing.T) {
	config := &SagaConfig{
		Fn: "arn1",
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
		msgs[0], msgs[1], msgs[3], msgs[5], msgs[6], msgs[7],
	}, config.records["1"])

	require.Equal(t, event.Msgs{
		msgs[2], msgs[4], msgs[8],
	}, config.records["2"])
}

func TestSagaClearAndHasEvent(t *testing.T) {
	config := &SagaConfig{
		Fn: "arn1",
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
	require.True(t, config.hasEvents())
}
