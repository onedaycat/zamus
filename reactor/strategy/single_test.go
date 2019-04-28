package strategy_test

import (
	"context"
	"testing"

	appErr "github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSingleHandler(t *testing.T) {
	s := setupSingle().
		WithSingleEvent().
		WithHandler("h1", ModeNormal).
		WithHandler("h2", ModeNormal).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.NoError(t, err)
	require.Equal(t, 2, s.spy.Count(EventTypes[0]))
	require.Equal(t, 0, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("h2"))
}

func TestSingleHandlerWithPreAndPost(t *testing.T) {
	s := setupSingle().
		WithSingleEvent().
		WithHandler("h1", ModeNormal).
		WithHandler("h2", ModeNormal).
		WithPreHandler("pre1", ModeNormal).
		WithPreHandler("pre2", ModeNormal).
		WithPostHandler("post1", ModeNormal).
		WithPostHandler("post2", ModeNormal).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.NoError(t, err)
	require.Equal(t, 2, s.spy.Count(EventTypes[0]))
	require.Equal(t, 0, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("h2"))
	require.Equal(t, 2, s.spy.Count("pre1"))
	require.Equal(t, 2, s.spy.Count("pre2"))
	require.Equal(t, 2, s.spy.Count("post1"))
	require.Equal(t, 2, s.spy.Count("post2"))
}

func TestSingleHandlerNoFilter(t *testing.T) {
	s := setupSingle().
		WithSingleEvent().
		WithHandler("h1", ModeNormal).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.NoError(t, err)
	require.Equal(t, 1, s.spy.Count(EventTypes[0]))
	require.Equal(t, 0, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
}

func TestSingleError(t *testing.T) {
	s := setupSingle().
		WithSingleEvent().
		WithHandler("h1", ModeError).
		WithPreHandler("pre1", ModeNormal).
		WithPostHandler("post1", ModeNormal).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Equal(t, 1, s.spy.Count(EventTypes[0]))
	require.Equal(t, 1, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("pre1"))
	require.Equal(t, 0, s.spy.Count("post1"))
}

func TestSinglePanic(t *testing.T) {
	t.Run("Panic Error", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModePanic).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModeNormal).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count(EventTypes[0]))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupSingle().
			WithHandler("h1", ModePanicString).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModeNormal).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})
}

func TestSinglePreError(t *testing.T) {
	s := setupSingle().
		WithSingleEvent().
		WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
		WithPreHandler("pre1", ModeError).
		WithPreHandler("pre2", ModeError).
		WithPostHandler("post1", ModeNormal).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Equal(t, 1, s.spy.Count("err"))
	require.Equal(t, 0, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("pre1"))
	require.Equal(t, 0, s.spy.Count("pre2"))
	require.Equal(t, 0, s.spy.Count("post1"))
}

func TestSinglePrePanic(t *testing.T) {
	t.Run("Panic Error", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
			WithPreHandler("pre1", ModePanic).
			WithPreHandler("pre2", ModePanic).
			WithPostHandler("post1", ModeNormal).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 0, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
			WithPreHandler("pre1", ModePanicString).
			WithPreHandler("pre2", ModePanicString).
			WithPostHandler("post1", ModeNormal).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 0, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
	})
}

func TestSinglePostError(t *testing.T) {
	s := setupSingle().
		WithSingleEvent().
		WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
		WithPreHandler("pre1", ModeNormal).
		WithPostHandler("post1", ModeError).
		WithPostHandler("post2", ModeError).
		WithError("err")

	err := s.strategy.Process(context.Background(), s.records)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Equal(t, 1, s.spy.Count(EventTypes[0]))
	require.Equal(t, 1, s.spy.Count("err"))
	require.Equal(t, 1, s.spy.Count("h1"))
	require.Equal(t, 1, s.spy.Count("pre1"))
	require.Equal(t, 1, s.spy.Count("post1"))
	require.Equal(t, 0, s.spy.Count("post2"))
}

func TestSinglePostPanic(t *testing.T) {
	t.Run("Panic Error", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeNormal).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModePanic).
			WithPostHandler("post2", ModePanic).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count(EventTypes[0]))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 0, s.spy.Count("post2"))
	})

	t.Run("Panic String", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeNormal).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModePanicString).
			WithPostHandler("post2", ModePanicString).
			WithError("err")

		err := s.strategy.Process(context.Background(), s.records)

		require.Equal(t, appErr.ErrPanic, err)
		require.Equal(t, 1, s.spy.Count(EventTypes[0]))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 1, s.spy.Count("post1"))
		require.Equal(t, 0, s.spy.Count("post2"))
	})
}

func TestSingleDLQ(t *testing.T) {
	t.Run("Retry 3", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeError).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModeNormal).
			WithError("err").
			WithDLQ(3)

		s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dlq.GetDLQErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 3, s.spy.Count(EventTypes[0]))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 3, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dlqMock.AssertExpectations(t)
	})

	t.Run("Retry 1", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeError).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModeNormal).
			WithError("err").
			WithDLQ(1)

		s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dlq.GetDLQErrors(), 1)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 1, s.spy.Count(EventTypes[0]))
		require.Equal(t, 1, s.spy.Count("err"))
		require.Equal(t, 1, s.spy.Count("h1"))
		require.Equal(t, 1, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dlqMock.AssertExpectations(t)
	})

	t.Run("Retry 3 on Panic", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModePanic).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModeNormal).
			WithError("err").
			WithDLQ(3)

		s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dlq.GetDLQErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 3, s.spy.Count(EventTypes[0]))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 3, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dlqMock.AssertExpectations(t)
	})

	t.Run("Retry 3 on Panic String", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModePanicString).
			WithPreHandler("pre1", ModeNormal).
			WithPostHandler("post1", ModeNormal).
			WithError("err").
			WithDLQ(3)

		s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dlq.GetDLQErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 3, s.spy.Count(EventTypes[0]))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 3, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dlqMock.AssertExpectations(t)
	})
}

func TestSinglePredDLQ(t *testing.T) {
	t.Run("Retry 3", func(t *testing.T) {
		s := setupSingle().
			WithSingleEvent().
			WithHandler("h1", ModeNormal).
			WithPreHandler("pre1", ModeError).
			WithPostHandler("post1", ModeNormal).
			WithError("err").
			WithDLQ(3)

		s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
			require.Len(t, s.dlq.GetDLQErrors(), 3)
		}).Return(nil)

		err := s.strategy.Process(context.Background(), s.records)

		require.NoError(t, err)
		require.Equal(t, 0, s.spy.Count(EventTypes[0]))
		require.Equal(t, 3, s.spy.Count("err"))
		require.Equal(t, 0, s.spy.Count("h1"))
		require.Equal(t, 3, s.spy.Count("pre1"))
		require.Equal(t, 0, s.spy.Count("post1"))
		s.dlqMock.AssertExpectations(t)
	})
}
