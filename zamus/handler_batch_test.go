package zamus

import (
    "context"
    "encoding/json"
    "testing"

    "github.com/onedaycat/errors"
    "github.com/stretchr/testify/require"
)

func TestBatchHandlerSuccess(t *testing.T) {
    h := New(&testHandler{
        batchHandler: func(ctx context.Context, sources interface{}) (interface{}, error) {
            src := sources.([]*testReq)
            return []*testRes{
                {Name: src[0].ID},
                {Name: src[1].ID},
            }, nil
        },
    })

    ctx := context.Background()
    result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

    require.NoError(t, err)
    require.Equal(t, []*testRes{
        {Name: "1"},
        {Name: "2"},
    }, result)
}

func TestBatchHandlerError(t *testing.T) {
    h := New(&testHandler{
        batchHandler: func(ctx context.Context, sources interface{}) (interface{}, error) {
            return nil, errors.InternalError("code1", "msg1")
        },
    })

    ctx := context.Background()
    result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

    require.NotNil(t, err)
    require.Equal(t, "code1: msg1", err.Error())
    require.Nil(t, result)
}

func TestBatchHandlerPanic(t *testing.T) {
    th := &testHandler{}
    h := New(th)

    t.Run("Panic with lambda.Error", func(t *testing.T) {
        th.batchHandler = func(ctx context.Context, source interface{}) (interface{}, error) {
            panic(errors.InternalError("code1", "msg1"))
            return nil, nil
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestBatchHandlerPanicWithPanicHandler(t *testing.T) {
    h := New(&testHandler{
        batchHandler: func(ctx context.Context, source interface{}) (interface{}, error) {
            panic(errors.InternalError("code1", "msg1"))
            return nil, nil
        },
    })

    t.Run("With Response", func(t *testing.T) {
        h.OnPanicHandler(func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error) {
            return &testRes{Name: "error"}, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "error"}, result)
    })

    t.Run("With Custom error", func(t *testing.T) {
        h.OnPanicHandler(func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error) {
            return nil, errors.InternalError("notfound", "msg1")
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "notfound: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestBatchHandlerPreHandler(t *testing.T) {
    h := New(&testHandler{
        batchHandler: func(ctx context.Context, sources interface{}) (interface{}, error) {
            src := sources.([]*testReq)
            return []*testRes{
                {Name: src[0].ID},
                {Name: src[1].ID},
            }, nil
        },
    })

    t.Run("Forward to Handler", func(t *testing.T) {
        h.batchPreHandlers = nil
        h.RegisterBatchPreHandler(func(ctx context.Context, src interface{}) (interface{}, error) {
            return nil, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NoError(t, err)
        require.Equal(t, []*testRes{
            {Name: "1"},
            {Name: "2"},
        }, result)
    })

    t.Run("Response", func(t *testing.T) {
        h.batchPreHandlers = nil
        h.RegisterBatchPreHandler(func(ctx context.Context, src interface{}) (interface{}, error) {
            return &testRes{Name: "2"}, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "2"}, result)
    })

    t.Run("Error", func(t *testing.T) {
        h.batchPreHandlers = nil
        h.RegisterBatchPreHandler(func(ctx context.Context, src interface{}) (interface{}, error) {
            return nil, errors.InternalError("code1", "msg1")
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestBatchHandlerPostHandler(t *testing.T) {
    h := New(&testHandler{
        batchHandler: func(ctx context.Context, sources interface{}) (interface{}, error) {
            src := sources.([]*testReq)
            return []*testRes{
                {Name: src[0].ID},
                {Name: src[1].ID},
            }, nil
        },
    })

    t.Run("Forward to post processor", func(t *testing.T) {
        h.postHandlers = nil
        h.RegisterBatchPostHandler(func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error) {
            return res, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NoError(t, err)
        require.Equal(t, []*testRes{
            {Name: "1"},
            {Name: "2"},
        }, result)
    })

    t.Run("Transform Response", func(t *testing.T) {
        h.postHandlers = nil
        h.RegisterBatchPostHandler(func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error) {
            x := res.([]*testRes)
            x[0].Name = "3"
            return x, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NoError(t, err)
        require.Equal(t, []*testRes{
            {Name: "3"},
            {Name: "2"},
        }, result)
    })

    t.Run("Transform response to error", func(t *testing.T) {
        h.postHandlers = nil
        h.RegisterBatchPostHandler(func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error) {
            return nil, errors.InternalError("code1", "msg1")
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestBatchHandlerRetryHandler(t *testing.T) {
    called := 0
    th := &testHandler{
        batchHandler: func(ctx context.Context, sources interface{}) (interface{}, error) {
            called++
            return nil, errors.InternalError("code1", "msg1")
        },
    }
    h := New(th)

    t.Run("Retry 2 times", func(t *testing.T) {
        called = 0
        h.SetRetry(2)

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
        require.Equal(t, 3, called)
    })

    t.Run("Retry 1 times", func(t *testing.T) {
        called = 0
        h.SetRetry(1)

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
        require.Equal(t, 2, called)
    })

    t.Run("Retry and pass", func(t *testing.T) {
        called = 0
        h.SetRetry(1)

        th.batchHandler = func(ctx context.Context, sources interface{}) (interface{}, error) {
            if called == 1 {
                src := sources.([]*testReq)
                return []*testRes{
                    {Name: src[0].ID},
                    {Name: src[1].ID},
                }, nil
            }

            called++
            return nil, errors.InternalError("code1", "msg1")
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NoError(t, err)
        require.Equal(t, []*testRes{
            {Name: "1"},
            {Name: "2"},
        }, result)
        require.Equal(t, 1, called)
    })

    t.Run("Retry failed Handler", func(t *testing.T) {
        called = 0
        h.SetRetry(1)
        h.OnRetryFailedHandler(func(ctx context.Context, src interface{}, err error) (interface{}, error) {
            return nil, errors.InternalError("code2", "msg2")
        })

        th.batchHandler = func(ctx context.Context, sources interface{}) (interface{}, error) {
            called++
            return nil, errors.InternalError("code1", "msg1")
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`[{"id":"1"},{"id":"2"}]`))

        require.NotNil(t, err)
        require.Equal(t, "code2: msg2", err.Error())
        require.Nil(t, result)
        require.Equal(t, 2, called)
    })
}
