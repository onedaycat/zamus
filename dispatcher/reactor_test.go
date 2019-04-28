package dispatcher

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/invoke/mocks"
	"github.com/onedaycat/zamus/random"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

func TestReactorFilterAndPublish(t *testing.T) {
	config := &ReactorConfig{
		Fn: "arn1",
		FilterEvents: event.EventTypes(
			(*domain.StockItemCreated)(nil),
			(*domain.StockItemRemoved)(nil),
		),
	}
	config.init()

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

	for _, msg := range msgs {
		config.filter(msg)
	}

	require.Len(t, config.records.Msgs, 5)
	for i, msg := range config.records.Msgs {
		require.Equal(t, msgs[i], msg)
	}

	mockClient := &mocks.Invoker{}
	config.client = mockClient
	config.setContext(context.Background())

	req := invoke.NewReactorRequest(config.Fn).WithEventList(config.records)

	mockClient.On("InvokeReactor", config.ctx, req).Return(nil).Once()
	err := config.publish()
	require.NoError(t, err)

	mockClient.On("InvokeReactor", config.ctx, req).Return(errors.DumbError).Once()
	err = config.publish()
	require.NoError(t, err)
}

func TestReactorAsync(t *testing.T) {
	config := &ReactorConfig{
		Fn:    "arn1",
		Async: true,
		FilterEvents: event.EventTypes(
			(*domain.StockItemCreated)(nil),
			(*domain.StockItemRemoved)(nil),
		),
	}
	config.init()

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

	for _, msg := range msgs {
		config.filter(msg)
	}

	require.Len(t, config.records.Msgs, 5)
	for i, msg := range config.records.Msgs {
		require.Equal(t, msgs[i], msg)
	}

	mockClient := &mocks.Invoker{}
	config.client = mockClient
	config.setContext(context.Background())

	req := invoke.NewReactorRequest(config.Fn).WithEventList(config.records)

	mockClient.On("InvokeReactorAsync", config.ctx, req).Return(nil).Once()
	err := config.publish()
	require.NoError(t, err)

	mockClient.On("InvokeReactorAsync", config.ctx, req).Return(errors.DumbError).Once()
	err = config.publish()
	require.NoError(t, err)
}

func TestReactorAllEvents(t *testing.T) {
	config := &ReactorConfig{
		Fn: "arn1",
	}
	config.init()
	require.True(t, config.isAll)

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

	for _, msg := range msgs {
		config.filter(msg)
	}

	require.Len(t, config.records.Msgs, 9)
}

func TestReactorClearAndHasEvent(t *testing.T) {
	config := &ReactorConfig{
		Fn: "arn1",
	}
	config.init()
	require.True(t, config.isAll)

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

	for _, msg := range msgs {
		config.filter(msg)
	}

	require.True(t, config.hasEvents())
	config.clear()
	require.True(t, config.hasEvents())
}
