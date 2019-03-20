package service_test

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/service"
	"github.com/stretchr/testify/require"
)

const (
	MODE_NORMAL = iota
	MODE_NORMAL_INPRE_RETURN
	MODE_PANIC
	MODE_PANIC_STRING
	MODE_ERROR
)

type HandlerSuite struct {
	h       *service.ServiceHandler
	spy     *common.SpyTest
	result  map[string]interface{}
	result2 map[string]interface{}
	results []map[string]interface{}
}

func setupHandlerSuite() *HandlerSuite {
	s := &HandlerSuite{}
	s.h = service.NewHandler(&service.Config{
		SentryDNS:   "test",
		EnableTrace: true,
	})
	s.h.ErrorHandlers(service.PrintPanic)
	s.spy = common.Spy()
	s.result = map[string]interface{}{
		"id":   "1",
		"name": "hello",
	}
	s.result2 = map[string]interface{}{
		"id":   "2",
		"name": "hello2",
	}

	s.results = []map[string]interface{}{
		{"id": "1", "name": "n1"},
		{"id": "2", "name": "n2"},
		{"id": "3", "name": "n3"},
		{"id": "4", "name": "n4"},
		{"id": "5", "name": "n5"},
	}

	s.h.ErrorHandlers(func(ctx context.Context, req *service.Request, err errors.Error) {
		s.spy.Called("err")
	})

	return s
}

func (s *HandlerSuite) WithHandler(name string, mode int, result interface{}, preName ...string) *HandlerSuite {
	var prehandlers []service.Handler
	if len(preName) > 0 {
		prehandlers = make([]service.Handler, len(preName))
		for i := range preName {
			pre := preName[i]
			prehandlers[i] = func(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
				s.spy.Called(pre)

				switch mode {
				case MODE_PANIC:
					panic(appErr.ErrInternalError)
				case MODE_PANIC_STRING:
					panic("string")
				case MODE_ERROR:
					return nil, appErr.ErrInternalError
				case MODE_NORMAL_INPRE_RETURN:
					return s.result2, nil
				}

				return nil, nil
			}
		}
	}

	s.h.RegisterHandler(name, func(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return nil, appErr.ErrInternalError
		}

		return result, nil
	}, service.WithPrehandler(prehandlers...))

	return s
}

func (s *HandlerSuite) WithMergeHandler(name string, mode int) *HandlerSuite {
	s.h.RegisterHandler(name, func(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
		s.spy.Called(name)

		return nil, nil
	}, service.WithMergeBatchHandler(func(ctx context.Context, req *service.Request, results service.BatchResults) errors.Error {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return appErr.ErrInternalError
		}

		args := BatchArgsList{}
		if err := req.ParseArgs(&args); err != nil {
			return err
		}

		for i := 0; i < len(args); i++ {
			results[i].Data = args[i]
		}

		return nil
	}))

	return s
}

func (s *HandlerSuite) WithBatchHandler(name string, mode int) *HandlerSuite {
	s.h.RegisterHandler(name, func(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return nil, appErr.ErrInternalError
		}

		args := &BatchArgs{}
		if err := req.ParseArgs(args); err != nil {
			return nil, err
		}

		return args, nil
	})

	return s
}

func (s *HandlerSuite) WithPreHandler(name string, mode int, result interface{}) *HandlerSuite {
	s.h.PreHandlers(func(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return nil, appErr.ErrInternalError
		}

		return result, nil
	})

	return s
}

func (s *HandlerSuite) WithPostHandler(name string, mode int, result interface{}) *HandlerSuite {
	s.h.PostHandlers(func(ctx context.Context, req *service.Request) (interface{}, errors.Error) {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return nil, appErr.ErrInternalError
		}

		return result, nil
	})

	return s
}

func TestHandler(t *testing.T) {
	s := setupHandlerSuite()
	req := random.ServiceReq("f1").Build()

	t.Run("Success", func(t *testing.T) {
		s.WithHandler("f1", MODE_NORMAL, s.result)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result, res)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("Error", func(t *testing.T) {
		s.WithHandler("f1", MODE_ERROR, s.result)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrInternalError, err)
		require.Len(t, res, 0)
		require.Equal(t, 2, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("err"))
	})

	t.Run("Panic", func(t *testing.T) {
		s.WithHandler("f1", MODE_PANIC, s.result)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrPanic, err)
		require.Len(t, res, 0)
		require.Equal(t, 3, s.spy.Count("f1"))
		require.Equal(t, 2, s.spy.Count("err"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s.WithHandler("f1", MODE_PANIC_STRING, s.result)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrPanic, err)
		require.Len(t, res, 0)
		require.Equal(t, 4, s.spy.Count("f1"))
		require.Equal(t, 3, s.spy.Count("err"))
	})

	t.Run("No Req", func(t *testing.T) {
		s.WithHandler("f1", MODE_NORMAL, s.result)
		res, err := s.h.Handle(context.Background(), nil)

		require.Equal(t, appErr.ErrUnableParseRequest, err)
		require.Nil(t, res)
		require.Equal(t, 4, s.spy.Count("f1"))
		require.Equal(t, 3, s.spy.Count("err"))
	})

	t.Run("NotFound", func(t *testing.T) {
		s.WithHandler("f1", MODE_NORMAL, s.result)

		req.Function = "f2"
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrFunctionNotFound("f2"), err)
		require.Len(t, res, 0)
		require.Equal(t, 4, s.spy.Count("f1"))
		require.Equal(t, 3, s.spy.Count("err"))
	})
}

func TestWarmer(t *testing.T) {
	s := setupHandlerSuite()
	s.WithHandler("f1", MODE_NORMAL, s.result)

	req := random.ServiceReq("f1").Warmer().Build()

	res := make(map[string]interface{})
	err := s.h.Run(context.Background(), req, &res)

	require.NoError(t, err)
	require.Len(t, res, 0)
	require.Equal(t, 0, s.spy.Count("f1"))
	require.Equal(t, 0, s.spy.Count("err"))
}

func TestInPreHandler(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result, "pre1", "pre2")
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result, res)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("pre2"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("PreHandlerReturn", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL_INPRE_RETURN, s.result, "pre1", "pre2")
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result2, res)
		require.Equal(t, 0, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("pre2"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("Error", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_ERROR, s.result, "pre1", "pre2")
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrInternalError, err)
		require.Len(t, res, 0)
		require.Equal(t, 0, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("pre2"))
		require.Equal(t, 1, s.spy.Count("err"))
	})

	t.Run("Panic", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_PANIC, s.result, "pre1", "pre2")
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrPanic.Code, err.GetCode())
		require.Len(t, res, 0)
		require.Equal(t, 0, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("pre2"))
		require.Equal(t, 1, s.spy.Count("err"))
	})
}

func TestPreHandler(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPreHandler("pre1", MODE_NORMAL, nil).
			WithPreHandler("pre2", MODE_NORMAL, nil)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result, res)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("pre2"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("PreHandlerReturn", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPreHandler("pre1", MODE_NORMAL, nil).
			WithPreHandler("pre2", MODE_NORMAL, s.result2)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result2, res)
		require.Equal(t, 0, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("pre2"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("Error", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPreHandler("pre1", MODE_ERROR, nil).
			WithPreHandler("pre2", MODE_NORMAL, nil)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrInternalError, err)
		require.Len(t, res, 0)
		require.Equal(t, 0, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("pre2"))
		require.Equal(t, 1, s.spy.Count("err"))
	})

	t.Run("Panic", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPreHandler("pre1", MODE_NORMAL, nil).
			WithPreHandler("pre2", MODE_PANIC, nil)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrPanic.Code, err.GetCode())
		require.Len(t, res, 0)
		require.Equal(t, 0, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("pre2"))
		require.Equal(t, 1, s.spy.Count("err"))
	})
}

func TestPostHandler(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPostHandler("post1", MODE_NORMAL, nil).
			WithPostHandler("post2", MODE_NORMAL, nil)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result, res)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 1, s.spy.Count("post2"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("PostHandlerReturn", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPostHandler("post1", MODE_NORMAL, nil).
			WithPostHandler("post2", MODE_NORMAL, s.result2)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.NoError(t, err)
		require.Equal(t, s.result2, res)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 1, s.spy.Count("post2"))
		require.Equal(t, 0, s.spy.Count("err"))
	})

	t.Run("Error", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPostHandler("post1", MODE_ERROR, nil).
			WithPostHandler("post2", MODE_NORMAL, nil)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrInternalError, err)
		require.Len(t, res, 0)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 0, s.spy.Count("post2"))
		require.Equal(t, 1, s.spy.Count("err"))
	})

	t.Run("Panic", func(t *testing.T) {
		s := setupHandlerSuite()
		req := random.ServiceReq("f1").Build()
		s.WithHandler("f1", MODE_NORMAL, s.result).
			WithPostHandler("post1", MODE_NORMAL, nil).
			WithPostHandler("post2", MODE_PANIC, nil)
		res := make(map[string]interface{})
		err := s.h.Run(context.Background(), req, &res)

		require.Equal(t, appErr.ErrPanic.Code, err.GetCode())
		require.Len(t, res, 0)
		require.Equal(t, 1, s.spy.Count("f1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 1, s.spy.Count("post2"))
		require.Equal(t, 1, s.spy.Count("err"))
	})
}
