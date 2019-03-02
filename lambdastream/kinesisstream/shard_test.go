package kinesisstream

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/onedaycat/errors"
	"github.com/stretchr/testify/require"
)

// func TestShardStream(t *testing.T) {
// 	data := ""

// 	randMsgs := random.EventMsgs()
// 	for i := 0; i < 20; i++ {
// 		randMsgs.
// 			Add("eid", data, random.WithAggregateID("a"+strconv.Itoa(i))).
// 			Add("eid", data, random.WithAggregateID("a"+strconv.Itoa(i)))
// 	}
// 	msgs := randMsgs.Build()

// 	randKs := random.KinesisEvents()
// 	for _, msg := range msgs {
// 		randKs.Add(msg.AggregateID, msg)
// 	}
// 	ksevent := randKs.Build()

// 	ks := kinesisstream.NewShardStrategy(33)
// 	ks.FilterEvents("eid")
// 	ks.Process(ksevent.Records)

// }

func init() {
	runtime.GOMAXPROCS(12)
}

func BenchmarkShard(b *testing.B) {
	handler := func(ctx context.Context, msgs EventMsgs) error {
		time.Sleep(time.Millisecond * 20)
		return nil
	}

	records := make(Records, 100)
	for i := 0; i < 100; i++ {
		rec := &Record{}
		istr := strconv.Itoa(i)
		rec.add(istr, istr, "et1")
		records[i] = rec
	}

	h := NewShardStrategy(3)
	h.RegisterHandlers(handler, handler, handler)
	h.FilterEvents("et1")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h.Process(context.Background(), records)
	}
}

func BenchmarkPartition(b *testing.B) {

	handler := func(ctx context.Context, msgs EventMsgs) error {
		time.Sleep(time.Millisecond * 20)
		return nil
	}

	records := make(Records, 100)
	for i := 0; i < 100; i++ {
		rec := &Record{}
		istr := strconv.Itoa(i)
		pkstr := strconv.Itoa(i % 10)
		rec.add(pkstr, istr, "et1")
		records[i] = rec
	}

	h := NewPartitionStrategy()
	h.RegisterHandlers(handler, handler, handler)
	h.FilterEvents("et1")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h.Process(context.Background(), records)
	}
}

func BenchmarkSimple(b *testing.B) {

	handler := func(ctx context.Context, msgs EventMsgs) error {
		time.Sleep(time.Millisecond * 20)
		return nil
	}

	records := make(Records, 100)
	for i := 0; i < 100; i++ {
		rec := &Record{}
		istr := strconv.Itoa(i)
		rec.add(istr, istr, "et1")
		records[i] = rec
	}

	h := NewSimpleStrategy()
	h.RegisterHandlers(handler, handler, handler)
	h.FilterEvents("et1")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h.Process(context.Background(), records)
	}
}

func TestShardStrategy(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) error {
		for _, msg := range msgs {
			if msg.EventType == "et1" {
				h1ET1++
			}
			if msg.EventType == "et3" {
				h1ET3++
			}
			fmt.Println("h1", msg.EventID, msg.EventType)
		}
		return nil
	}

	handler2 := func(ctx context.Context, msgs EventMsgs) error {
		for _, msg := range msgs {
			if msg.EventType == "et1" {
				h2ET1++
			}
			if msg.EventType == "et3" {
				h2ET3++
			}
			fmt.Println("h2", msg.EventID, msg.EventType)
		}
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		nError++
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler1, handler2)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.NoError(t, err)
	require.Equal(t, 0, nError)
	require.Equal(t, 3, h1ET1)
	require.Equal(t, 0, h1ET2)
	require.Equal(t, 3, h1ET3)
	require.Equal(t, 3, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 3, h2ET3)
}

func TestShardStrategyError(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) error {
		for _, msg := range msgs {
			if msg.EventType == "et1" {
				h1ET1++
			}
			if msg.EventType == "et3" {
				h1ET3++
			}
			fmt.Println("h1", msg.EventID, msg.EventType)
		}
		return nil
	}

	handler2 := func(ctx context.Context, msgs EventMsgs) error {
		for _, msg := range msgs {
			fmt.Println("h2", msg.EventID, msg.EventType)
			if msg.EventType == "et1" {
				h2ET1++
			}
			if msg.EventType == "et3" {
				h2ET3++
			}
			if msg.EventID == "4" {
				return errors.InternalError("ABC", "error on 4").WithCaller()
			}
		}
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler1, handler2)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.Error(t, err)
	require.Equal(t, 1, nError)
	require.Equal(t, 3, h1ET1)
	require.Equal(t, 0, h1ET2)
	require.Equal(t, 3, h1ET3)
	require.Equal(t, 2, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 3, h2ET3)
}

func TestShardStrategyPanic(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) error {
		for _, msg := range msgs {
			if msg.EventType == "et1" {
				h1ET1++
			}
			if msg.EventType == "et3" {
				h1ET3++
			}
			fmt.Println("h1", msg.EventID, msg.EventType)
		}
		return nil
	}

	handler2 := func(ctx context.Context, msgs EventMsgs) error {
		for _, msg := range msgs {
			fmt.Println("h2", msg.EventID, msg.EventType)
			if msg.EventType == "et1" {
				h2ET1++
			}
			if msg.EventType == "et3" {
				h2ET3++
			}

			if msg.EventID == "4" {
				var x *KinesisStreamEvent
				_ = x.Records
			}
		}
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler1, handler2)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.Error(t, err)
	require.Equal(t, 1, nError)
	require.Equal(t, 3, h1ET1)
	require.Equal(t, 0, h1ET2)
	require.Equal(t, 3, h1ET3)
	require.Equal(t, 2, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 3, h2ET3)
}

func TestShardStrategyPanicPre(t *testing.T) {
	nError := 0
	isPre := false
	prehandler := func(ctx context.Context, msgs EventMsgs) error {
		isPre = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler)
	cm.PreHandlers(prehandler)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.Error(t, err)
	require.Equal(t, 1, nError)
	require.True(t, isPre)
}

func TestShardStrategyPanicPost(t *testing.T) {
	isError := false
	isPost := false
	posthandler := func(ctx context.Context, msgs EventMsgs) error {
		isPost = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		isError = true
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler)
	cm.PostHandlers(posthandler)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.Error(t, err)
	require.True(t, isError)
	require.True(t, isPost)
}

func TestShardStrategyPanicPreWithPost(t *testing.T) {
	isError := false
	isPost := false
	isPre := false

	prehandler := func(ctx context.Context, msgs EventMsgs) error {
		isPre = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	posthandler := func(ctx context.Context, msgs EventMsgs) error {
		isPost = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		isError = true
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler)
	cm.PreHandlers(prehandler)
	cm.PostHandlers(posthandler)
	cm.FilterEvents("et1")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.Error(t, err)
	require.True(t, isError)
	require.True(t, isPre)
	require.False(t, isPost)
}

func TestShardStrategyPanicPostWithPre(t *testing.T) {
	isError := false
	isPost := false
	isPre := false

	prehandler := func(ctx context.Context, msgs EventMsgs) error {
		isPre = true
		return nil
	}

	posthandler := func(ctx context.Context, msgs EventMsgs) error {
		isPost = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err error) {
		isError = true
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewShardStrategy(10)
	cm.RegisterHandlers(handler)
	cm.PreHandlers(prehandler)
	cm.PostHandlers(posthandler)
	cm.FilterEvents("et1")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	err := cm.Process(context.Background(), records)
	require.Error(t, err)
	require.True(t, isError)
	require.True(t, isPre)
	require.True(t, isPost)
}
