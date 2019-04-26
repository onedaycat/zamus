package publisher

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/zamus/event"
	invokeMock "github.com/onedaycat/zamus/invoke/mocks"
	kinMock "github.com/onedaycat/zamus/publisher/mocks"
	"github.com/onedaycat/zamus/random"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestInvokeSuccess(t *testing.T) {
	mockKin := &kinMock.KinesisPublisher{}
	mockInvoke := &invokeMock.Invoker{}
	h := New(mockInvoke, mockKin)
	h.Kinesis(&KinesisConfig{
		StreamARN: "arn1",
		FilterEvents: event.EventTypes(
			(*domain.StockItemCreated)(nil),
			(*domain.StockItemRemoved)(nil),
		),
	})
	h.Reactor(&ReactorConfig{
		Fn: "arn1",
		FilterEvents: event.EventTypes(
			(*domain.StockItemCreated)(nil),
			(*domain.StockItemRemoved)(nil),
		),
	})
	h.Saga(&SagaConfig{
		Fn: "arn1",
		FilterEvents: event.EventTypes(
			(*domain.StockItemCreated)(nil),
		),
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

	dy := random.DynamoDB().Add(msgs...).Build()

	mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, nil).Once()
	mockInvoke.On("InvokeReactor", mock.Anything, mock.Anything).Return(nil).Once()
	mockInvoke.On("InvokeSaga", mock.Anything, mock.Anything, nil).Return(nil).Times(3)

	err := h.Handle(context.Background(), dy)
	require.NoError(t, err)

	mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, nil).Once()
	mockInvoke.On("InvokeReactor", mock.Anything, mock.Anything).Return(nil).Once()
	mockInvoke.On("InvokeSaga", mock.Anything, mock.Anything, nil).Return(nil).Times(3)

	err = h.Handle(context.Background(), dy)
	require.NoError(t, err)
}
