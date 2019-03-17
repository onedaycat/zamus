package kinesisstream_test

import (
	"context"
	"testing"

	appErr "github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSimpleHandler(t *testing.T) {
	s := setupSimple().
		WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_3).
		WithHandler("h2", MODE_NORMAL, EVENT_TYPE_3).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.NoError(t, err)
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
	require.Equal(t, 0, s.spy.Count(EVENT_TYPE_2))
	require.Equal(t, 6, s.spy.Count(EVENT_TYPE_3))
	require.Equal(t, 0, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("h2"))
}

func TestSimpleHandlerWithPreAndPost(t *testing.T) {
	s := setupSimple().
		WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_3).
		WithHandler("h2", MODE_NORMAL, EVENT_TYPE_3).
		WithPreHandler("pre1", MODE_NORMAL).
		WithPreHandler("pre2", MODE_NORMAL).
		WithPostHandler("post1", MODE_NORMAL).
		WithPostHandler("post2", MODE_NORMAL).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.NoError(t, err)
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
	require.Equal(t, 0, s.spy.Count(EVENT_TYPE_2))
	require.Equal(t, 6, s.spy.Count(EVENT_TYPE_3))
	require.Equal(t, 0, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("h2"))
	require.Equal(t, 2, s.spy.Count("pre1"))
	require.Equal(t, 2, s.spy.Count("pre2"))
	require.Equal(t, 2, s.spy.Count("post1"))
	require.Equal(t, 2, s.spy.Count("post2"))
}

func TestSimpleHandlerNoFilter(t *testing.T) {
	s := setupSimple().
		WithHandler("h1", MODE_NORMAL).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.NoError(t, err)
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_3))
	require.Equal(t, 0, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
}

func TestSimpleError(t *testing.T) {
	s := setupSimple().
		WithHandler("h1", MODE_ERROR, EVENT_TYPE_1, EVENT_TYPE_2).
		WithPreHandler("pre1", MODE_NORMAL).
		WithPostHandler("post1", MODE_NORMAL).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
	require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
	require.Equal(t, 1, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("pre1"))
	require.Equal(t, 0, s.spy.Count("post1"))
}

func TestSimplePanic(t *testing.T) {
	t.Run("Panic Error", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_PANIC, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_PANIC_STRING, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})
}

func TestSimplePreError(t *testing.T) {
	s := setupSimple().
		WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
		WithPreHandler("pre1", MODE_ERROR).
		WithPreHandler("pre2", MODE_ERROR).
		WithPostHandler("post1", MODE_NORMAL).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Equal(t, 1, s.spy.Count("err"))
	require.Equal(t, 0, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("pre1"))
	require.Equal(t, 0, s.spy.Count("pre2"))
	require.Equal(t, 0, s.spy.Count("post1"))
}

func TestSimplePrePanic(t *testing.T) {
	t.Run("Panic Error", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_PANIC).
			WithPreHandler("pre2", MODE_PANIC).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 0, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_PANIC_STRING).
			WithPreHandler("pre2", MODE_PANIC_STRING).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 0, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})
}

func TestSimplePostError(t *testing.T) {
	s := setupSimple().
		WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
		WithPreHandler("pre1", MODE_NORMAL).
		WithPostHandler("post1", MODE_ERROR).
		WithPostHandler("post2", MODE_ERROR).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
	require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
	require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
	require.Equal(t, 1, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("pre1"))
	require.Equal(t, 1, s.spy.Count("post1"))
	require.Equal(t, 0, s.spy.Count("post2"))
}

func TestSimplePostPanic(t *testing.T) {
	t.Run("Panic Error", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_PANIC).
			WithPostHandler("post2", MODE_PANIC).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 0, s.spy.Count("post2"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_PANIC_STRING).
			WithPostHandler("post2", MODE_PANIC_STRING).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 0, s.spy.Count("post2"))
	})
}

func TestSimpleDQL(t *testing.T) {
	t.Run("Retry 3", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_ERROR, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err").
			WithDQL(3)

		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dql.GetDQLErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 9, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 9, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 3, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dqlMock.AssertExpectations(t)
	})

	t.Run("Retry 1", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_ERROR, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err").
			WithDQL(1)

		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dql.GetDQLErrors(), 1)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 3, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dqlMock.AssertExpectations(t)
	})

	t.Run("Retry 3 on Panic", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_PANIC, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err").
			WithDQL(3)

		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dql.GetDQLErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 9, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 9, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 3, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dqlMock.AssertExpectations(t)
	})

	t.Run("Retry 3 on Panic String", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_PANIC_STRING, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_NORMAL).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err").
			WithDQL(3)

		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dql.GetDQLErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 9, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 9, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 3, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dqlMock.AssertExpectations(t)
	})
}

func TestSimplePredDQL(t *testing.T) {
	t.Run("Retry 3", func(t *testing.T) {
		s := setupSimple().
			WithHandler("h1", MODE_NORMAL, EVENT_TYPE_1, EVENT_TYPE_2).
			WithPreHandler("pre1", MODE_ERROR).
			WithPostHandler("post1", MODE_NORMAL).
			WithError("err").
			WithDQL(3)

		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dql.GetDQLErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_1))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_2))
		require.Equal(t, 0, s.spy.Count(EVENT_TYPE_3))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 0, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dqlMock.AssertExpectations(t)
	})
}
