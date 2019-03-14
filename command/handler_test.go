package command_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

type CommandHandlerSuite struct {
	*common.SpyTest
	handler *command.Handler
	ctx     context.Context
}

func setupCommandHandler() *CommandHandlerSuite {
	s := &CommandHandlerSuite{}
	s.SpyTest = common.Spy()

	s.handler = command.NewHandler(&command.Config{})
	s.ctx = context.Background()

	return s
}

func (s *CommandHandlerSuite) WithHandler(function string) *CommandHandlerSuite {
	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		cmd := &domain.CreateStockCmd{}
		err := req.ParseArgs(cmd)
		if err != nil {
			panic(err)
		}
		s.Called(function)
		return nil, nil
	}

	fErr := func(ctx context.Context, req *command.CommandReq, xerr errors.Error) {
		s.Called(function + "Err")
	}

	s.handler.RegisterCommand(function, f)
	s.handler.ErrorHandlers(fErr)

	return s
}

func (s *CommandHandlerSuite) WithErrorHandler(function string, err errors.Error) *CommandHandlerSuite {
	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		s.Called(function)
		return nil, err
	}

	fErr := func(ctx context.Context, req *command.CommandReq, xerr errors.Error) {
		s.Called(function + "Err")
	}

	s.handler.RegisterCommand(function, f)
	s.handler.ErrorHandlers(fErr)

	return s
}

func (s *CommandHandlerSuite) WithPanicErrorHandler(t *testing.T, req *command.CommandReq, function string, err errors.Error) *CommandHandlerSuite {
	f := func(ctx context.Context, xreq *command.CommandReq) (interface{}, errors.Error) {
		require.Equal(t, req, xreq)
		s.Called(function)
		panic(err)
	}

	fErr := func(ctx context.Context, xreq *command.CommandReq, xerr errors.Error) {
		require.Equal(t, req, xreq)
		require.Equal(t, appErr.ErrInternalError, xerr)
		s.Called(function + "Err")
	}

	s.handler.RegisterCommand(function, f)
	s.handler.ErrorHandlers(fErr)

	return s
}

func (s *CommandHandlerSuite) WithPanicHandler(t *testing.T, req *command.CommandReq, function string) *CommandHandlerSuite {
	f := func(ctx context.Context, xreq *command.CommandReq) (interface{}, errors.Error) {
		require.Equal(t, req, xreq)
		s.Called(function)
		panic("panic string")
	}

	fErr := func(ctx context.Context, xreq *command.CommandReq, xerr errors.Error) {
		require.Equal(t, req, xreq)
		require.Equal(t, appErr.ErrInternalError, xerr)
		s.Called(function + "Err")
	}

	s.handler.RegisterCommand(function, f)
	s.handler.ErrorHandlers(fErr)

	return s
}

func Test_Command_Handler(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s := setupCommandHandler().
			WithHandler("f")

		req := random.CommandReq().
			Function("f").
			Arg(domain.CreateStockCmd{"p1", 1}).
			Build()

		resp, err := s.handler.Handle(s.ctx, req)

		require.Nil(t, err)
		require.Nil(t, resp)
		require.Equal(t, 1, s.Count("f"))
		require.Equal(t, 0, s.Count("fErr"))
	})

	t.Run("Not Found", func(t *testing.T) {
		s := setupCommandHandler()

		req := random.CommandReq().
			Function("xxxxxx").
			Build()

		resp, err := s.handler.Handle(s.ctx, req)

		require.Error(t, appErr.ErrCommandNotFound("xxxxxx"), err)
		require.Nil(t, resp)
		require.Equal(t, 0, s.Count("f"))
		require.Equal(t, 0, s.Count("fErr"))
	})

	t.Run("Error", func(t *testing.T) {
		s := setupCommandHandler().
			WithErrorHandler("f", appErr.ErrUnableMarshal)

		req := random.CommandReq().
			Function("f").
			Build()

		resp, err := s.handler.Handle(s.ctx, req)

		require.Error(t, appErr.ErrUnableMarshal, err)
		require.Nil(t, resp)
		require.Equal(t, 1, s.Count("f"))
		require.Equal(t, 1, s.Count("fErr"))
	})

	t.Run("Panic Error", func(t *testing.T) {
		s := setupCommandHandler()

		req := random.CommandReq().
			Function("f").
			Build()

		s.WithPanicErrorHandler(t, req, "f", appErr.ErrUnableMarshal)

		resp, err := s.handler.Handle(s.ctx, req)

		require.Error(t, appErr.ErrPanic, err)
		require.Nil(t, resp)
		require.Equal(t, 1, s.Count("f"))
		require.Equal(t, 1, s.Count("fErr"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupCommandHandler()

		req := random.CommandReq().
			Function("f").
			Build()

		s.WithPanicHandler(t, req, "f")

		resp, err := s.handler.Handle(s.ctx, req)

		require.Error(t, appErr.ErrPanic, err)
		require.Nil(t, resp)
		require.Equal(t, 1, s.Count("f"))
		require.Equal(t, 1, s.Count("fErr"))
	})
}

type PreCommandHandlerSuite struct {
	*common.SpyTest
	handler *command.Handler
	ctx     context.Context
}

func setupPreCommandHandler() *PreCommandHandlerSuite {
	s := &PreCommandHandlerSuite{}
	s.SpyTest = common.Spy()

	s.handler = command.NewHandler(&command.Config{})
	s.ctx = context.Background()

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		s.Called("f")
		return nil, nil
	}

	s.handler.RegisterCommand("f", f)

	return s
}

func (s *PreCommandHandlerSuite) WithPreHandler(function string, n int) *PreCommandHandlerSuite {
	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		s.Called(function)
		return nil, nil
	}

	s.handler.PreHandlers(f)

	return s
}

func Test_Pre_Command_Handler(t *testing.T) {
	spy := common.Spy()

	req := &command.CommandReq{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	fPre := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("fPre")
		return nil, nil
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		spy.Called("fErr")
	}

	h := command.NewHandler(&command.Config{})

	h.PreHandlers(fPre, fPre)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), req)

	require.Nil(t, err)
	require.Equal(t, nil, resp)
	require.Equal(t, 1, spy.Count("f"))
	require.Equal(t, 2, spy.Count("fPre"))
	require.Equal(t, 0, spy.Count("fErr"))
}

func TestCommandPreHandlerError(t *testing.T) {
	spy := common.Spy()

	req := &command.CommandReq{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	fPre := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("fPre")
		return nil, nil
	}

	fPreErr := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("fPreErr")
		return nil, appErr.ErrUnknown
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		spy.Called("fErr")
	}

	h := command.NewHandler(&command.Config{})

	h.PreHandlers(fPreErr, fPre)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), req)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Equal(t, nil, resp)
	require.Equal(t, 0, spy.Count("f"))
	require.Equal(t, 0, spy.Count("fPre"))
	require.Equal(t, 1, spy.Count("fPreErr"))
	require.Equal(t, 1, spy.Count("fErr"))
}

func TestCommandPreHandlerResult(t *testing.T) {
	spy := common.Spy()

	req := &command.CommandReq{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("f")
		return nil, nil
	}

	fPre := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("fPre")
		return nil, nil
	}

	fPre2 := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		spy.Called("fPre")
		return 1, nil
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		spy.Called("fErr")
	}

	h := command.NewHandler(&command.Config{})

	h.PreHandlers(fPre, fPre2)
	h.RegisterCommand("testHandlerCommand", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), req)

	require.Nil(t, err)
	require.Equal(t, 1, resp)
	require.Equal(t, 0, spy.Count("f"))
	require.Equal(t, 2, spy.Count("fPre"))
	require.Equal(t, 0, spy.Count("fErr"))
}

func TestErrorHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	req := &command.CommandReq{
		Function:      "testHandlerCommandError",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "deleteWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		require.False(t, checkFunc)
		checkFunc = true
		return nil, appErr.ErrUnknown
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		errorFunc = true
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommandError", f, command.WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), req)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestPanicHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	req := &command.CommandReq{
		Function:      "testHandlerCommandError",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "deleteWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		require.False(t, checkFunc)
		checkFunc = true
		var e *command.CommandReq
		_ = e.Args

		return nil, nil
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		errorFunc = true
		fmt.Println(err)
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommandError", f, command.WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), req)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestPanicStringHandler(t *testing.T) {
	checkFunc := false
	errorFunc := false

	req := &command.CommandReq{
		Function:      "testHandlerCommandError",
		PermissionKey: "workspace_1",
		Identity: &invoke.Identity{
			Claims: invoke.Claims{
				Permissions: invoke.Permissions{
					"workspace_1": "deleteWorkspace",
				},
			},
		},
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		require.False(t, checkFunc)
		checkFunc = true
		panic("hello")
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		errorFunc = true
		fmt.Println(err)
	}

	h := command.NewHandler(&command.Config{})
	h.RegisterCommand("testHandlerCommandError", f, command.WithPermission("deleteWorkspace"))
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), req)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Nil(t, resp)
	require.True(t, checkFunc)
	require.True(t, errorFunc)
}

func TestCommandPostHandler(t *testing.T) {
	nH := 0
	nFPost1 := 0
	nFPost2 := 0

	req := &command.CommandReq{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		nH++
		return nil, nil
	}

	fPost1 := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		nFPost1++
		return 1, nil
	}

	fPost2 := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		nFPost2++
		return nil, nil
	}

	h := command.NewHandler(&command.Config{})
	h.PostHandlers(fPost1, fPost2)
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.Handle(context.Background(), req)

	require.Nil(t, err)
	require.Equal(t, 1, resp)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nFPost1)
	require.Equal(t, 0, nFPost2)
}

func TestCommandPostHandlerError(t *testing.T) {
	nH := 0
	nFPost1 := 0
	nFPost2 := 0
	nFErr := 0

	req := &command.CommandReq{
		Function: "testHandlerCommand",
	}

	f := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		nH++
		return nil, nil
	}

	fPost1 := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		nFPost1++
		return nil, appErr.ErrUnknown
	}

	fPost2 := func(ctx context.Context, req *command.CommandReq) (interface{}, errors.Error) {
		nFPost2++
		return nil, nil
	}

	fErr := func(ctx context.Context, req *command.CommandReq, err errors.Error) {
		nFErr++
	}

	h := command.NewHandler(&command.Config{})
	h.PostHandlers(fPost1, fPost2)
	h.ErrorHandlers(fErr)
	h.RegisterCommand("testHandlerCommand", f)

	resp, err := h.Handle(context.Background(), req)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nH)
	require.Equal(t, 1, nFPost1)
	require.Equal(t, 0, nFPost2)
}

func BenchmarkHandlerSuccess(b *testing.B) {
	s := setupCommandHandler().
		WithHandler("f")

	req := random.CommandReq().
		Function("f").
		Arg(domain.CreateStockCmd{"p1", 1}).
		Build()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.handler.Handle(s.ctx, req)
	}
}
