package invoke

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/stretchr/testify/require"
)

func TestInvokeHandler(t *testing.T) {
	data := []string{"1", "2", "3"}
	fn2Err := errors.InternalError("fn2", "fn2error")

	fn1 := func(ctx context.Context, event *InvokeEvent) *Result {
		return event.Result(data)
	}

	fn2 := func(ctx context.Context, event *InvokeEvent) *Result {
		return event.ErrorResult(fn2Err)
	}

	fn3 := func(ctx context.Context, event *InvokeEvent) *Result {
		return nil
	}

	e := NewEventManager()
	e.RegisterInvoke("fn1", fn1, nil, nil)
	e.RegisterInvoke("fn2", fn2, nil, nil)
	e.RegisterInvoke("fn3", fn3, nil, nil)

	req1 := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn1",
		},
	}

	req2 := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn2",
		},
	}

	req3 := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn3",
		},
	}

	result1, err := e.Run(context.Background(), req1)
	require.NoError(t, err)
	require.Equal(t, &Result{
		Data:  data,
		Error: nil,
	}, result1)

	result2, err := e.Run(context.Background(), req2)
	require.NoError(t, err)
	require.Equal(t, &Result{
		Data:  nil,
		Error: fn2Err,
	}, result2)

	result3, err := e.Run(context.Background(), req3)
	require.NoError(t, err)
	require.Equal(t, &Result{
		Data:  nil,
		Error: ErrNoResult,
	}, result3)
}
