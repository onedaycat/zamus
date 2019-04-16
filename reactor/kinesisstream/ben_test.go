package kinesisstream_test

import (
    "context"
    "testing"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/random"
    "github.com/onedaycat/zamus/reactor/kinesisstream"
)

func BenchmarkPartition(b *testing.B) {
    h := func(ctx context.Context, msgs []*eventstore.EventMsg) errors.Error {
        // time.Sleep(100 * time.Millisecond)
        return nil
    }

    ksevent := random.KinesisEvents().RandomMessage(100).Build()
    eventTypes := make([]string, 0, 100)
    for _, x := range ksevent.Records {
        eventTypes = append(eventTypes, x.Kinesis.Data.EventMsg.EventType)
    }

    cm := kinesisstream.NewPartitionStrategy()
    cm.RegisterHandler(h, eventTypes)

    ctx := context.Background()

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = cm.Process(ctx, ksevent.Records)
    }
}

func BenchmarkSimple(b *testing.B) {
    h := func(ctx context.Context, msgs []*eventstore.EventMsg) errors.Error {
        // time.Sleep(100 * time.Millisecond)
        return nil
    }

    ksevent := random.KinesisEvents().RandomMessage(100).Build()
    eventTypes := make([]string, 0, 100)
    for _, x := range ksevent.Records {
        eventTypes = append(eventTypes, x.Kinesis.Data.EventMsg.EventType)
    }

    cm := kinesisstream.NewSimpleStrategy()
    cm.RegisterHandler(h, eventTypes)

    ctx := context.Background()

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = cm.Process(ctx, ksevent.Records)
    }
}

func BenchmarkShard(b *testing.B) {
    h := func(ctx context.Context, msgs []*eventstore.EventMsg) errors.Error {
        // time.Sleep(100 * time.Millisecond)
        return nil
    }

    ksevent := random.KinesisEvents().RandomMessage(100).Build()
    eventTypes := make([]string, 0, 100)
    for _, x := range ksevent.Records {
        eventTypes = append(eventTypes, x.Kinesis.Data.EventMsg.EventType)
    }

    cm := kinesisstream.NewShardStrategy()
    cm.RegisterHandler(h, eventTypes)

    ctx := context.Background()

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = cm.Process(ctx, ksevent.Records)
    }
}
