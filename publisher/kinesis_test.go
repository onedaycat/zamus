package publisher

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go/service/kinesis"
    "github.com/onedaycat/errors"
    mockKinPub "github.com/onedaycat/zamus/publisher/mocks"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
)

func TestProcessKinesisSuccess(t *testing.T) {
    mockKin := &mockKinPub.KinesisPublisher{}

    h := New(Config{
        Kinesis: &KinesisConfig{
            Client:     mockKin,
            StreamARNs: []string{"arn1", "arn2"},
        },
    })

    msgs := random.EventMsgs().RandomEventMsgs(1).Build()
    dyRecs := random.DynamoDB().Add(msgs...).BuildJSON()

    mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, nil).Once()
    mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, nil).Once()

    res, err := h.Invoke(context.Background(), dyRecs)
    require.NoError(t, err)
    require.Nil(t, res)
}

func TestProcessKinesisError(t *testing.T) {
    mockKin := &mockKinPub.KinesisPublisher{}

    h := New(Config{
        Kinesis: &KinesisConfig{
            Client:     mockKin,
            StreamARNs: []string{"arn1", "arn2"},
        },
    })

    msgs := random.EventMsgs().RandomEventMsgs(1).Build()
    dyRecs := random.DynamoDB().Add(msgs...).Build()

    mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, errors.DumbError).Times(1)
    mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, nil).Times(1)

    err := h.processKinesis(context.Background(), dyRecs)
    require.True(t, ErrUnablePublishKinesis.Is(err))
}

func TestProcessKinesisOutError(t *testing.T) {
    mockKin := &mockKinPub.KinesisPublisher{}

    h := New(Config{
        Kinesis: &KinesisConfig{
            Client:     mockKin,
            StreamARNs: []string{"arn1", "arn2"},
        },
    })

    msgs := random.EventMsgs().RandomEventMsgs(1).Build()
    dyRecs := random.DynamoDB().Add(msgs...).Build()

    errCount := int64(1)
    mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{
        FailedRecordCount: &errCount,
    }, nil).Times(1)
    mockKin.On("PutRecordsWithContext", mock.Anything, mock.Anything).Return(&kinesis.PutRecordsOutput{}, nil).Times(1)

    err := h.processKinesis(context.Background(), dyRecs)
    require.True(t, ErrUnablePublishKinesis.Is(err))
}
