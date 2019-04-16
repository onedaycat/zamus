package main

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go/service/kinesis"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

func TestHandle(t *testing.T) {
    var err error
    msgs := random.EventMsgs().RandomEventMsgs(10, random.WithMetadata(eventstore.NewMetadata().SetUserID("1"))).Build()
    event := random.DynamoDB().Add(msgs...).Build()

    h := &Handler{
        records: make([]*kinesis.PutRecordsRequestEntry, 0, 200),
    }

    err = h.Process(context.Background(), event)
    require.NoError(t, err)
    require.Len(t, h.records, 10)
    require.Equal(t, 10, h.count)
    require.Len(t, h.records[:h.count], 10)
    for i := 0; i < h.count; i++ {
        require.Equal(t, *h.records[i].PartitionKey, msgs[i].AggregateID)
    }

    msgs = random.EventMsgs().RandomEventMsgs(3, random.WithMetadata(eventstore.NewMetadata().SetUserID("1"))).Build()
    event = random.DynamoDB().Add(msgs...).Build()
    err = h.Process(context.Background(), event)
    require.NoError(t, err)
    require.Len(t, h.records, 10)
    require.Equal(t, 3, h.count)
    require.Len(t, h.records[:h.count], 3)
    for i := 0; i < h.count; i++ {
        require.Equal(t, *h.records[i].PartitionKey, msgs[i].AggregateID)
    }

    msgs = random.EventMsgs().RandomEventMsgs(100, random.WithMetadata(eventstore.NewMetadata().SetUserID("1"))).Build()
    event = random.DynamoDB().Add(msgs...).Build()

    err = h.Process(context.Background(), event)
    require.NoError(t, err)
    require.Len(t, h.records, 100)
    require.Equal(t, 100, h.count)
    require.Len(t, h.records[:h.count], 100)
    for i := 0; i < h.count; i++ {
        require.Equal(t, *h.records[i].PartitionKey, msgs[i].AggregateID)
    }

    msgs = random.EventMsgs().RandomEventMsgs(3, random.WithMetadata(eventstore.NewMetadata().SetUserID("1"))).Build()
    event = random.DynamoDB().Add(msgs...).Build()
    err = h.Process(context.Background(), event)
    require.NoError(t, err)
    require.Len(t, h.records, 100)
    require.Equal(t, 3, h.count)
    require.Len(t, h.records[:h.count], 3)
    for i := 0; i < h.count; i++ {
        require.Equal(t, *h.records[i].PartitionKey, msgs[i].AggregateID)
    }
}

func BenchmarkProcess(b *testing.B) {
    msgs := random.EventMsgs().RandomEventMsgs(200, random.WithMetadata(eventstore.NewMetadata().SetUserID("1"))).Build()
    event := random.DynamoDB().Add(msgs...).Build()

    h := &Handler{
        records: make([]*kinesis.PutRecordsRequestEntry, 0, 200),
    }

    b.ReportAllocs()
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        h.Process(context.Background(), event)
    }
}
