package kinesisstream

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/dql/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPartitionStrategy(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, nil)
	cm.RegisterHandler(handler2, nil)
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

func TestPartitionStrategyWithFilter(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0

	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
		for _, msg := range msgs {
			h2ET3++
			fmt.Println("h2", msg.EventID, msg.EventType)
		}
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, func() []string { return []string{"et1", "et3"} })
	cm.RegisterHandler(handler2, func() []string { return []string{"et3"} })
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
	require.Equal(t, 0, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 3, h2ET3)
}

func TestPartitionStrategyError(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, nil)
	cm.RegisterHandler(handler2, nil)
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

func TestPartitionStrategyPanic(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, nil)
	cm.RegisterHandler(handler2, nil)
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

func TestPartitionStrategyPanicPre(t *testing.T) {
	nError := 0
	isPre := false
	prehandler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		isPre = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler, nil)
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

func TestPartitionStrategyPanicPost(t *testing.T) {
	isError := false
	isPost := false
	posthandler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		isPost = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		isError = true
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler, nil)
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

func TestPartitionStrategyPanicPreWithPost(t *testing.T) {
	isError := false
	isPost := false
	isPre := false

	prehandler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		isPre = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	posthandler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		isPost = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		isError = true
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler, nil)
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

func TestPartitionStrategyPanicPostWithPre(t *testing.T) {
	isError := false
	isPost := false
	isPre := false

	prehandler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		isPre = true
		return nil
	}

	posthandler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		isPost = true
		if msgs[0].EventType == "et1" {
			var x *KinesisStreamEvent
			_ = x.Records
		}
		return nil
	}

	handler := func(ctx context.Context, msgs EventMsgs) errors.Error {
		return nil
	}

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		isError = true
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler, nil)
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

func TestPartitionStrategyErrorWithRetry(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, nil)
	cm.RegisterHandler(handler2, nil)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)
	dqlStorage := &mocks.Storage{}
	d := dql.New(dqlStorage, 3, "srv1", "fn1", "1.0.0")
	cm.SetDQL(d)
	dqlStorage.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
		require.Len(t, d.Errors, 3)
	}).Return(nil)

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
	require.Equal(t, 3, nError)
	require.Equal(t, 9, h1ET1)
	require.Equal(t, 0, h1ET2)
	require.Equal(t, 9, h1ET3)
	require.Equal(t, 6, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 9, h2ET3)
}

func TestPartitionStrategyErrorWithRetryOnce(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, nil)
	cm.RegisterHandler(handler2, nil)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)
	dqlStorage := &mocks.Storage{}
	d := dql.New(dqlStorage, 1, "srv1", "fn1", "1.0.0")
	cm.SetDQL(d)
	dqlStorage.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
		require.Len(t, d.Errors, 1)
	}).Return(nil)

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
	require.Equal(t, 1, nError)
	require.Equal(t, 3, h1ET1)
	require.Equal(t, 0, h1ET2)
	require.Equal(t, 3, h1ET3)
	require.Equal(t, 2, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 3, h2ET3)
}

func TestPartitionStrategyPanicWithRetry(t *testing.T) {
	nError := 0
	h1ET1 := 0
	h1ET2 := 0
	h1ET3 := 0
	h2ET1 := 0
	h2ET2 := 0
	h2ET3 := 0
	handler1 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	handler2 := func(ctx context.Context, msgs EventMsgs) errors.Error {
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

	onErr := func(ctx context.Context, msgs EventMsgs, err errors.Error) {
		nError++
		fmt.Println("Error Trigger", err)
	}

	n := 10
	cm := NewPartitionStrategy()
	cm.RegisterHandler(handler1, nil)
	cm.RegisterHandler(handler2, nil)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)
	dqlStorage := &mocks.Storage{}
	d := dql.New(dqlStorage, 3, "srv1", "fn1", "1.0.0")
	cm.SetDQL(d)
	dqlStorage.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
		require.Len(t, d.Errors, 3)
	}).Return(nil)

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
	require.Equal(t, 3, nError)
	require.Equal(t, 9, h1ET1)
	require.Equal(t, 0, h1ET2)
	require.Equal(t, 9, h1ET3)
	require.Equal(t, 6, h2ET1)
	require.Equal(t, 0, h2ET2)
	require.Equal(t, 9, h2ET3)
}
