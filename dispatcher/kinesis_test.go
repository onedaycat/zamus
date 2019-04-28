package dispatcher

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/dispatcher/mocks"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/random"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

func TestKinesisFilterAndPublish(t *testing.T) {
	config := &KinesisConfig{
		StreamARN: "arn1",
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

	require.Len(t, config.records, 5)
	for i, record := range config.records {
		data, _ := event.MarshalMsg(msgs[i])
		require.Equal(t, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &msgs[i].AggID,
		}, record)
	}

	mockClient := &mocks.KinesisPublisher{}
	config.client = mockClient
	config.setContext(context.Background())

	input := &kinesis.PutRecordsInput{
		Records:    config.records,
		StreamName: &config.StreamARN,
	}

	mockClient.On("PutRecordsWithContext", config.ctx, input).Return(&kinesis.PutRecordsOutput{}, nil).Once()
	err := config.publish()
	require.NoError(t, err)

	mockClient.On("PutRecordsWithContext", config.ctx, input).Return(&kinesis.PutRecordsOutput{}, errors.DumbError).Once()
	err = config.publish()
	require.True(t, ErrUnablePublishKinesis.Is(err))

	failedCount := int64(10)
	mockClient.On("PutRecordsWithContext", config.ctx, input).Return(&kinesis.PutRecordsOutput{
		FailedRecordCount: &failedCount,
	}, nil).Once()
	err = config.publish()
	require.True(t, ErrUnablePublishKinesis.Is(err))

	mockClient.AssertExpectations(t)
}

func TestKinesisAllEvents(t *testing.T) {
	config := &KinesisConfig{
		StreamARN: "arn1",
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

	require.Len(t, config.records, 9)
}

func TestKinesisClearAndHasEvent(t *testing.T) {
	config := &KinesisConfig{
		StreamARN: "arn1",
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
