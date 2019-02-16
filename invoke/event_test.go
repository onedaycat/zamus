package invoke

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/stretchr/testify/require"
)

func TestParseBatchInvokeEvent(t *testing.T) {

	testcases := []struct {
		payload  string
		expEvent *BatchInvokeEvent
	}{
		{
			`[{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"}},
			{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"},{"namespace": "2"}]`), &Identity{Sub: "xx"}, 2},
		},
		// no field
		{
			`[{"arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"}},
			{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"},{"namespace": "2"}]`), &Identity{Sub: "xx"}, 2},
		},
		// no args
		{
			`[{"field": "testField1","source": {"namespace": "1"},"identity": {"sub": "xx"}},
			{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"testField1", nil, []byte(`[{"namespace": "1"},{"namespace": "2"}]`), &Identity{Sub: "xx"}, 2},
		},
		// no identity
		{
			`[{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"}},
			{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"},{"namespace": "2"}]`), nil, 2},
		},
		// missing source 1
		{
			`[{"field": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"}},
			{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "2"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "2"}]`), &Identity{Sub: "xx"}, 1},
		},
		// missing source 2
		{
			`[{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"}},
			{"field": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"testField1", []byte(`{"arg1": "1"}`), []byte(`[{"namespace": "1"}]`), &Identity{Sub: "xx"}, 1},
		},
		// no source
		{
			`[{"field": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"}},
			{"field": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"}}]`,
			&BatchInvokeEvent{"testField1", []byte(`{"arg1": "1"}`), nil, &Identity{Sub: "xx"}, 0},
		},
	}

	for _, testcase := range testcases {
		req := &Request{}
		err := json.Unmarshal([]byte(testcase.payload), req)
		require.NoError(t, err)
		require.Equal(t, testcase.expEvent, req.BatchInvokeEvent)
	}
}

func TestParseInvokeEvent(t *testing.T) {
	testcases := []struct {
		payload  string
		expEvent *InvokeEvent
	}{
		{
			`{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"}}`,
			&InvokeEvent{"testField1", []byte(`{"arg1": "1"}`), []byte(`{"namespace": "1"}`), &Identity{Sub: "xx"}},
		},
		// no field
		{
			`{"arguments": {"arg1": "1"},"source": {"namespace": "1"},"identity": {"sub": "xx"}}`,
			&InvokeEvent{"", []byte(`{"arg1": "1"}`), []byte(`{"namespace": "1"}`), &Identity{Sub: "xx"}},
		},
		// no args
		{
			`{"field": "testField1","source": {"namespace": "1"},"identity": {"sub": "xx"}}`,
			&InvokeEvent{"testField1", nil, []byte(`{"namespace": "1"}`), &Identity{Sub: "xx"}},
		},
		// no identity
		{
			`{"field": "testField1","arguments": {"arg1": "1"},"source": {"namespace": "1"}}`,
			&InvokeEvent{"testField1", []byte(`{"arg1": "1"}`), []byte(`{"namespace": "1"}`), nil},
		},
		// no source
		{
			`{"field": "testField1","arguments": {"arg1": "1"},"identity": {"sub": "xx"}}`,
			&InvokeEvent{"testField1", []byte(`{"arg1": "1"}`), nil, &Identity{Sub: "xx"}},
		},
	}

	for i, testcase := range testcases {
		req := &Request{}
		err := json.Unmarshal([]byte(testcase.payload), req)
		require.NoError(t, err)
		require.Equal(t, testcase.expEvent, req.InvokeEvent, i)
	}
}

func TestInvokeErrorHandler(t *testing.T) {
	fnErr := errors.InternalError("fn", "fnerror")
	isRunErr := false

	fn1 := func(ctx context.Context, event *InvokeEvent) *Result {
		isRunErr = true
		return event.ErrorResult(fnErr)
	}

	errfn := func(ctx context.Context, event *InvokeEvent, err error) {
		require.Equal(t, fnErr, err)
	}

	e := NewEventManager()
	e.RegisterInvoke("fn1", fn1, nil, nil)
	e.OnInvokeError(errfn)

	req1 := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn1",
		},
	}

	result1, err := e.Run(context.Background(), req1)
	require.NoError(t, err)
	require.Equal(t, &Result{
		Data:  nil,
		Error: fnErr,
	}, result1)
	require.True(t, isRunErr)
}

func TestBatchInvokeErrorHandler(t *testing.T) {
	fnErr := errors.InternalError("fn", "fnerror")
	isRunErr := false

	fn1 := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		isRunErr = true
		return event.ErrorResult(fnErr)
	}

	errfn := func(ctx context.Context, event *BatchInvokeEvent, err error) {
		require.Equal(t, fnErr, err)
	}

	e := NewEventManager()
	e.RegisterBatchInvoke("fn1", fn1, nil, nil)
	e.OnBatchInvokeError(errfn)

	req1 := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn1",
			NSource: 3,
		},
	}

	result1, err := e.Run(context.Background(), req1)
	require.NoError(t, err)
	require.Equal(t, []*Result{
		{nil, fnErr},
		{nil, fnErr},
		{nil, fnErr},
	}, result1)
	require.True(t, isRunErr)
}

func TestFieldNotFound(t *testing.T) {
	e := NewEventManager()

	reqBatchInvoke := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn1",
			NSource: 3,
		},
	}

	batchInvoke, err := e.Run(context.Background(), reqBatchInvoke)
	require.NoError(t, err)
	require.Equal(t, []*Result{
		{nil, ErrFuncNotFound("fn1")},
		{nil, ErrFuncNotFound("fn1")},
		{nil, ErrFuncNotFound("fn1")},
	}, batchInvoke)

	reqInvoke := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn1",
		},
	}

	invoke, err := e.Run(context.Background(), reqInvoke)
	require.NoError(t, err)
	require.Equal(t, &Result{
		Error: ErrFuncNotFound("fn1"),
	}, invoke)
}

func TestInvokePreHandler(t *testing.T) {
	data := "1"
	fnErr := errors.InternalError("fn", "fnerror")
	isPreRun := false
	isPreErrRun := false
	isHandlerRun := false
	isErrRun := false

	req := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn",
		},
	}

	prefn := func(ctx context.Context, event *InvokeEvent) error {
		isPreRun = true
		return nil
	}

	prefnErr := func(ctx context.Context, event *InvokeEvent) error {
		isPreErrRun = true
		return fnErr
	}

	errfn := func(ctx context.Context, event *InvokeEvent, err error) {
		isErrRun = true
	}

	fn := func(ctx context.Context, event *InvokeEvent) *Result {
		isHandlerRun = true
		return event.Result(data)
	}

	t.Run("Global without error", func(t *testing.T) {
		isPreRun = false
		isPreErrRun = false
		isHandlerRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, nil, nil)
		e.UseInvokePreHandler(prefn)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  data,
			Error: nil,
		}, result1)
		require.True(t, isPreRun)
		require.True(t, isHandlerRun)
		require.False(t, isErrRun)
		require.False(t, isPreErrRun)
	})

	t.Run("Global with error", func(t *testing.T) {
		isPreRun = false
		isPreErrRun = false
		isHandlerRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, nil, nil)
		e.UseInvokePreHandler(prefn, prefnErr)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  nil,
			Error: fnErr,
		}, result1)
		require.True(t, isPreRun)
		require.False(t, isHandlerRun)
		require.True(t, isErrRun)
		require.True(t, isPreErrRun)
		require.True(t, isPreErrRun)
	})

	t.Run("In handler with error", func(t *testing.T) {
		isPreRun = false
		isPreErrRun = false
		isHandlerRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, []InvokePreHandler{prefnErr}, nil)
		e.UseInvokePreHandler(prefn)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  nil,
			Error: fnErr,
		}, result1)
		require.True(t, isPreRun)
		require.False(t, isHandlerRun)
		require.True(t, isErrRun)
		require.True(t, isPreErrRun)
		require.True(t, isPreErrRun)
	})
}

func TestBatchInvokePreHandler(t *testing.T) {
	data := "1"
	fnErr := errors.InternalError("fn", "fnerror")
	isPreRun := false
	isPreErrRun := false
	isHandlerRun := false
	isErrRun := false

	req := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn",
			NSource: 1,
		},
	}

	prefn := func(ctx context.Context, event *BatchInvokeEvent) error {
		isPreRun = true
		return nil
	}

	prefnErr := func(ctx context.Context, event *BatchInvokeEvent) error {
		isPreErrRun = true
		return fnErr
	}

	errfn := func(ctx context.Context, event *BatchInvokeEvent, err error) {
		isErrRun = true
	}

	fn := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		isHandlerRun = true
		return event.Result([]*Result{
			{data, nil},
		})
	}

	t.Run("Global without error", func(t *testing.T) {
		isPreRun = false
		isPreErrRun = false
		isHandlerRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, nil, nil)
		e.UseBatchInvokePreHandler(prefn)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []*Result{
			{data, nil},
		}, result1)
		require.True(t, isPreRun)
		require.True(t, isHandlerRun)
		require.False(t, isErrRun)
		require.False(t, isPreErrRun)
	})

	t.Run("Global with error", func(t *testing.T) {
		isPreRun = false
		isPreErrRun = false
		isHandlerRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, nil, nil)
		e.UseBatchInvokePreHandler(prefn, prefnErr)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []*Result{
			{nil, fnErr},
		}, result1)
		require.True(t, isPreRun)
		require.False(t, isHandlerRun)
		require.True(t, isErrRun)
		require.True(t, isPreErrRun)
		require.True(t, isPreErrRun)
	})

	t.Run("In handler with error", func(t *testing.T) {
		isPreRun = false
		isPreErrRun = false
		isHandlerRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, []BatchInvokePreHandler{prefnErr}, nil)
		e.UseBatchInvokePreHandler(prefn)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []*Result{
			{nil, fnErr},
		}, result1)
		require.True(t, isPreRun)
		require.False(t, isHandlerRun)
		require.True(t, isErrRun)
		require.True(t, isPreErrRun)
		require.True(t, isPreErrRun)
	})
}

func TestInvokePostHandler(t *testing.T) {
	data := "1"
	resultErr := errors.InternalError("fn", "fnerror")
	fnErr := false
	localErr := false
	globalErr := false

	isLocalRun := false
	isGlobalRun := false
	isLocalErrRun := false
	isGlobalErrRun := false
	isHandlerRun := false
	isHandlerErrRun := false
	isErrRun := false

	req := &Request{
		eventType: eventInvokeType,
		InvokeEvent: &InvokeEvent{
			Function: "fn",
		},
	}

	localfn := func(ctx context.Context, event *InvokeEvent, result *Result) error {
		if localErr {
			isLocalRun = true
			isLocalErrRun = true
			return resultErr
		}
		isLocalRun = true
		isLocalErrRun = false

		return nil
	}

	globalfn := func(ctx context.Context, event *InvokeEvent, result *Result) error {
		if globalErr {
			isGlobalRun = true
			isGlobalErrRun = true
			return resultErr
		}
		isGlobalRun = true
		isGlobalErrRun = false

		return nil
	}

	errfn := func(ctx context.Context, event *InvokeEvent, err error) {
		isErrRun = true
	}

	fn := func(ctx context.Context, event *InvokeEvent) *Result {
		if fnErr {
			isHandlerRun = true
			isHandlerErrRun = true
			return event.ErrorResult(resultErr)
		}
		isHandlerRun = true
		isHandlerErrRun = false
		return event.Result(data)
	}

	t.Run("Handler error before and no error in post fn", func(t *testing.T) {
		fnErr = true
		localErr = false
		globalErr = false

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, nil, []InvokePostHandler{localfn})
		e.UseInvokePostHandler(globalfn)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  nil,
			Error: resultErr,
		}, result1)
		require.True(t, isLocalRun)
		require.False(t, isLocalErrRun)
		require.True(t, isGlobalRun)
		require.False(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.True(t, isHandlerErrRun)
		require.True(t, isErrRun)
	})

	t.Run("Handler error before and error on local", func(t *testing.T) {
		fnErr = true
		localErr = true
		globalErr = false

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, nil, []InvokePostHandler{localfn})
		e.UseInvokePostHandler(globalfn)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  nil,
			Error: resultErr,
		}, result1)
		require.True(t, isLocalRun)
		require.True(t, isLocalErrRun)
		require.False(t, isGlobalRun)
		require.False(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.True(t, isHandlerErrRun)
		require.True(t, isErrRun)
	})

	t.Run("Handler error before and error on global", func(t *testing.T) {
		fnErr = true
		localErr = false
		globalErr = true

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, nil, []InvokePostHandler{localfn})
		e.UseInvokePostHandler(globalfn)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  nil,
			Error: resultErr,
		}, result1)
		require.True(t, isLocalRun)
		require.False(t, isLocalErrRun)
		require.True(t, isGlobalRun)
		require.True(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.True(t, isHandlerErrRun)
		require.True(t, isErrRun)
	})

	t.Run("Handler and run local and post", func(t *testing.T) {
		fnErr = false
		localErr = false
		globalErr = false

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterInvoke("fn", fn, nil, []InvokePostHandler{localfn})
		e.UseInvokePostHandler(globalfn)
		e.OnInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, &Result{
			Data:  data,
			Error: nil,
		}, result1)
		require.True(t, isLocalRun)
		require.False(t, isLocalErrRun)
		require.True(t, isGlobalRun)
		require.False(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.False(t, isHandlerErrRun)
		require.False(t, isErrRun)
	})
}

func TestBatchInvokePostHandler(t *testing.T) {
	data := "1"
	resultErr := errors.InternalError("fn", "fnerror")
	fnErr := false
	localErr := false
	globalErr := false

	isLocalRun := false
	isGlobalRun := false
	isLocalErrRun := false
	isGlobalErrRun := false
	isHandlerRun := false
	isHandlerErrRun := false
	isErrRun := false

	req := &Request{
		eventType: eventBatchInvokeType,
		BatchInvokeEvent: &BatchInvokeEvent{
			Field:   "fn",
			NSource: 1,
		},
	}

	localfn := func(ctx context.Context, event *BatchInvokeEvent, results *Results) error {
		if localErr {
			isLocalRun = true
			isLocalErrRun = true
			return resultErr
		}
		isLocalRun = true
		isLocalErrRun = false

		return nil
	}

	globalfn := func(ctx context.Context, event *BatchInvokeEvent, results *Results) error {
		if globalErr {
			isGlobalRun = true
			isGlobalErrRun = true
			return resultErr
		}
		isGlobalRun = true
		isGlobalErrRun = false

		return nil
	}

	errfn := func(ctx context.Context, event *BatchInvokeEvent, err error) {
		isErrRun = true
	}

	fn := func(ctx context.Context, event *BatchInvokeEvent) *Results {
		if fnErr {
			isHandlerRun = true
			isHandlerErrRun = true
			return event.ErrorResult(resultErr)
		}
		isHandlerRun = true
		isHandlerErrRun = false
		return event.Result([]*Result{{data, nil}})
	}

	t.Run("Handler error before and no error in post fn", func(t *testing.T) {
		fnErr = true
		localErr = false
		globalErr = false

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, nil, []BatchInvokePostHandler{localfn})
		e.UseBatchInvokePostHandler(globalfn)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, makeErrorResults(1, resultErr).Results, result1)
		require.True(t, isLocalRun)
		require.False(t, isLocalErrRun)
		require.True(t, isGlobalRun)
		require.False(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.True(t, isHandlerErrRun)
		require.True(t, isErrRun)
	})

	t.Run("Handler error before and error on local", func(t *testing.T) {
		fnErr = true
		localErr = true
		globalErr = false

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, nil, []BatchInvokePostHandler{localfn})
		e.UseBatchInvokePostHandler(globalfn)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, makeErrorResults(1, resultErr).Results, result1)
		require.True(t, isLocalRun)
		require.True(t, isLocalErrRun)
		require.False(t, isGlobalRun)
		require.False(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.True(t, isHandlerErrRun)
		require.True(t, isErrRun)
	})

	t.Run("Handler error before and error on global", func(t *testing.T) {
		fnErr = true
		localErr = false
		globalErr = true

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, nil, []BatchInvokePostHandler{localfn})
		e.UseBatchInvokePostHandler(globalfn)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, makeErrorResults(1, resultErr).Results, result1)
		require.True(t, isLocalRun)
		require.False(t, isLocalErrRun)
		require.True(t, isGlobalRun)
		require.True(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.True(t, isHandlerErrRun)
		require.True(t, isErrRun)
	})

	t.Run("Handler and run local and post", func(t *testing.T) {
		fnErr = false
		localErr = false
		globalErr = false

		isLocalRun = false
		isGlobalRun = false
		isLocalErrRun = false
		isGlobalErrRun = false
		isHandlerRun = false
		isHandlerErrRun = false
		isErrRun = false

		e := NewEventManager()
		e.RegisterBatchInvoke("fn", fn, nil, []BatchInvokePostHandler{localfn})
		e.UseBatchInvokePostHandler(globalfn)
		e.OnBatchInvokeError(errfn)

		result1, err := e.Run(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []*Result{
			{data, nil},
		}, result1)
		require.True(t, isLocalRun)
		require.False(t, isLocalErrRun)
		require.True(t, isGlobalRun)
		require.False(t, isGlobalErrRun)
		require.True(t, isHandlerRun)
		require.False(t, isHandlerErrRun)
		require.False(t, isErrRun)
	})
}
