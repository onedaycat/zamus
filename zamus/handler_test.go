package zamus

import (
    "context"
    "encoding/json"
    "testing"

    "github.com/onedaycat/errors"
    "github.com/stretchr/testify/require"
)

type testReq struct {
    ID string
}

type testRes struct {
    Name string
}

type testHandler struct {
    handler      func(ctx context.Context, source interface{}) (interface{}, error)
    batchHandler func(ctx context.Context, sources interface{}) (interface{}, error)
}

func (h *testHandler) ParseSource(ctx context.Context, payload json.RawMessage) interface{} {
    source := &testReq{}
    err := jsonen.Unmarshal(payload, source)
    if err != nil {
        panic(err)
    }

    return source
}

func (h *testHandler) ParseSources(ctx context.Context, payload json.RawMessage) interface{} {
    sources := make([]*testReq, 0, 10)
    err := jsonen.Unmarshal(payload, &sources)
    if err != nil {
        panic(err)
    }

    return sources
}

func (h *testHandler) Handler(ctx context.Context, source interface{}) (interface{}, error) {
    if h.handler != nil {
        return h.handler(ctx, source)
    }

    return nil, nil
}

func (h *testHandler) BatchHandler(ctx context.Context, sources interface{}) (interface{}, error) {
    if h.batchHandler != nil {
        return h.batchHandler(ctx, sources)
    }

    return nil, nil
}

func TestHandlerSuccess(t *testing.T) {
    th := &testHandler{
        handler: func(ctx context.Context, source interface{}) (interface{}, error) {
            return &testRes{Name: source.(*testReq).ID}, nil
        },
    }
    h := New(th)

    t.Run("With Source", func(t *testing.T) {
        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "1"}, result)
    })
}

func TestHandlerError(t *testing.T) {
    h := New(&testHandler{
        handler: func(ctx context.Context, source interface{}) (interface{}, error) {
            return nil, errors.InternalError("code1", "msg1")
        },
    })

    ctx := context.Background()
    result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

    require.NotNil(t, err)
    require.Equal(t, "code1: msg1", err.Error())
    require.Nil(t, result)
}

func TestHandlerPanic(t *testing.T) {
    th := &testHandler{
    }
    h := New(th)

    t.Run("Panic with lambda.Error", func(t *testing.T) {
        th.handler = func(ctx context.Context, source interface{}) (interface{}, error) {
            panic(errors.InternalError("code1", "msg1"))
            return nil, nil
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
    })

    t.Run("Panic with error", func(t *testing.T) {
        th.handler = func(ctx context.Context, source interface{}) (interface{}, error) {
            panic(errors.New("msg1"))
            return nil, nil
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "msg1", err.Error())
        require.Nil(t, result)
    })

    t.Run("Panic with string", func(t *testing.T) {
        th.handler = func(ctx context.Context, source interface{}) (interface{}, error) {
            panic("msg1")
            return nil, nil
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "error: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestHandlerPanicWithPanicHandler(t *testing.T) {
    h := New(&testHandler{
        handler: func(ctx context.Context, source interface{}) (interface{}, error) {
            panic(errors.InternalError("code1", "msg1"))
            return nil, nil
        },
    })

    t.Run("With Response", func(t *testing.T) {
        h.OnPanicHandler(func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error) {
            return &testRes{Name: "error"}, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "error"}, result)
    })

    t.Run("With Custom error", func(t *testing.T) {
        h.OnPanicHandler(func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error) {
            return nil, errors.InternalError("notfound", "msg1")
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "notfound: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestHandlerPreHandler(t *testing.T) {
    h := New(&testHandler{
        handler: func(ctx context.Context, source interface{}) (interface{}, error) {
            return &testRes{Name: "1"}, nil
        },
    })

    t.Run("Forward to Handle", func(t *testing.T) {
        h.preHandlers = nil
        h.RegisterPreHandler(func(ctx context.Context, src interface{}) (interface{}, error) {
            return nil, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "1"}, result)
    })

    t.Run("Response", func(t *testing.T) {
        h.preHandlers = nil
        h.RegisterPreHandler(func(ctx context.Context, src interface{}) (interface{}, error) {
            return &testRes{Name: "2"}, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "2"}, result)
    })

    t.Run("Error", func(t *testing.T) {
        h.preHandlers = nil
        h.RegisterPreHandler(func(ctx context.Context, src interface{}) (interface{}, error) {
            return nil, errors.InternalError("code1", "msg1")
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestHandlerPostHandler(t *testing.T) {
    h := New(&testHandler{
        handler: func(ctx context.Context, source interface{}) (interface{}, error) {
            return &testRes{Name: "1"}, nil
        },
    })

    t.Run("Forward to post processor", func(t *testing.T) {
        h.postHandlers = nil
        h.RegisterPostHandler(func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error) {
            return res, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "1"}, result)
    })

    t.Run("Transform Response", func(t *testing.T) {
        h.postHandlers = nil
        h.RegisterPostHandler(func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error) {
            x := res.(*testRes)
            x.Name = "3"
            return x, nil
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "3"}, result)
    })

    t.Run("Transform response to error", func(t *testing.T) {
        h.postHandlers = nil
        h.RegisterPostHandler(func(ctx context.Context, src interface{}, res interface{}, err error) (interface{}, error) {
            return nil, errors.InternalError("code1", "msg1")
        })

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
    })
}

func TestHandlerRetryHandler(t *testing.T) {
    called := 0
    th := &testHandler{
        handler: func(ctx context.Context, source interface{}) (interface{}, error) {
            called++
            return nil, errors.InternalError("code1", "msg1")
        },
    }
    h := New(th)

    t.Run("Retry 2 times", func(t *testing.T) {
        called = 0
        h.SetRetry(2)

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
        require.Equal(t, 3, called)
    })

    t.Run("Retry 1 times", func(t *testing.T) {
        called = 0
        h.SetRetry(1)

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "code1: msg1", err.Error())
        require.Nil(t, result)
        require.Equal(t, 2, called)
    })

    t.Run("Retry and pass", func(t *testing.T) {
        called = 0
        h.SetRetry(1)
        th.handler = func(ctx context.Context, source interface{}) (interface{}, error) {
            if called == 1 {
                return &testRes{Name: "2"}, nil
            }

            called++
            return nil, errors.InternalError("code1", "msg1")
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, &testRes{Name: "2"}, result)
        require.Equal(t, 1, called)
    })

    t.Run("On retry failed Handle", func(t *testing.T) {
        called = 0
        h.SetRetry(1)
        h.OnRetryFailedHandler(func(ctx context.Context, payload json.RawMessage, err error) (interface{}, error) {
            return nil, errors.InternalError("code2", "msg2")
        })

        th.handler = func(ctx context.Context, source interface{}) (interface{}, error) {
            called++
            return nil, errors.InternalError("code1", "msg1")
        }

        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NotNil(t, err)
        require.Equal(t, "code2: msg2", err.Error())
        require.Nil(t, result)
        require.Equal(t, 2, called)
    })
}
