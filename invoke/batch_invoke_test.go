package invoke

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/stretchr/testify/require"
)

func TestBatchInvokeHandler(t *testing.T) {
	data := []string{"1", "2", "3"}
	fn2Err := errors.InternalError("fn2", "fn2error")

	fn1 := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		results := make([]*Result, event.NSource)

		for i := 0; i < event.NSource; i++ {
			results[i] = &Result{Data: data[i]}
		}

		return event.Result(results)
	}

	fn2 := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		return event.ErrorResult(fn2Err)
	}

	fn3 := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		results := make([]*Result, 2)

		for i := 0; i < 2; i++ {
			results[i] = &Result{Data: data[i]}
		}

		return event.Result(results)
	}

	fn4 := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		return nil
	}

	e := NewEventManager()
	e.RegisterBatchInvoke("fn1", fn1, nil, nil)
	e.RegisterBatchInvoke("fn2", fn2, nil, nil)
	e.RegisterBatchInvoke("fn3", fn3, nil, nil)
	e.RegisterBatchInvoke("fn4", fn4, nil, nil)

	req1 := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn1",
			NSource: 3,
		},
	}

	req2 := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn2",
			NSource: 3,
		},
	}

	req3 := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn3",
			NSource: 3,
		},
	}

	req4 := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn4",
			NSource: 3,
		},
	}

	result1, err := e.Run(context.Background(), req1)
	require.NoError(t, err)
	require.Equal(t, []*Result{
		{"1", nil},
		{"2", nil},
		{"3", nil},
	}, result1)

	result2, err := e.Run(context.Background(), req2)
	require.NoError(t, err)
	require.Equal(t, makeErrorResults(3, fn2Err).Results, result2)

	result3, err := e.Run(context.Background(), req3)
	require.NoError(t, err)
	require.Equal(t, []*Result{
		{nil, ErrBatchInvokeResultSizeNotMatch},
		{nil, ErrBatchInvokeResultSizeNotMatch},
		{nil, ErrBatchInvokeResultSizeNotMatch},
	}, result3)

	result4, err := e.Run(context.Background(), req4)
	require.NoError(t, err)
	require.Equal(t, []*Result{
		{nil, ErrNoResult},
		{nil, ErrNoResult},
		{nil, ErrNoResult},
	}, result4)
}
