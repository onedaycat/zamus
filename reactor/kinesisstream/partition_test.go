package kinesisstream_test

// import (
// 	"context"
// 	"testing"

// 	appErr "github.com/onedaycat/zamus/errors"
// 	"github.com/stretchr/testify/mock"
// 	"github.com/stretchr/testify/require"
// )

// func TestPartitionHandler(t *testing.T) {
// 	s := setupPartition().
// 		WithHandler("h1", ModeNormal, EventType1, EventType3).
// 		WithHandler("h2", ModeNormal, EventType3).
// 		WithError("err")

// 	err := s.strategy.Process(context.Background(), s.records)

// 	require.NoError(t, err)
// 	require.Equal(t, 3, s.spy.Count(EventType1))
// 	require.Equal(t, 0, s.spy.Count(EventType2))
// 	require.Equal(t, 6, s.spy.Count(EventType3))
// 	require.Equal(t, 0, s.spy.Count("err"))
// 	require.Equal(t, 2, s.spy.Count("h1"))
// 	require.Equal(t, 1, s.spy.Count("h2"))
// }

// func TestPartitionHandlerWithPreAndPost(t *testing.T) {
// 	s := setupPartition().
// 		WithHandler("h1", ModeNormal, EventType1, EventType3).
// 		WithHandler("h2", ModeNormal, EventType3).
// 		WithPreHandler("pre1", ModeNormal).
// 		WithPreHandler("pre2", ModeNormal).
// 		WithPostHandler("post1", ModeNormal).
// 		WithPostHandler("post2", ModeNormal).
// 		WithError("err")

// 	err := s.strategy.Process(context.Background(), s.records)

// 	require.NoError(t, err)
// 	require.Equal(t, 3, s.spy.Count(EventType1))
// 	require.Equal(t, 0, s.spy.Count(EventType2))
// 	require.Equal(t, 6, s.spy.Count(EventType3))
// 	require.Equal(t, 0, s.spy.Count("err"))
// 	require.Equal(t, 2, s.spy.Count("h1"))
// 	require.Equal(t, 1, s.spy.Count("h2"))
// 	require.Equal(t, 3, s.spy.Count("pre1"))
// 	require.Equal(t, 3, s.spy.Count("pre2"))
// 	require.Equal(t, 3, s.spy.Count("post1"))
// 	require.Equal(t, 3, s.spy.Count("post2"))
// }

// func TestPartitionHandlerNoFilter(t *testing.T) {
// 	s := setupPartition().
// 		WithHandler("h1", ModeNormal).
// 		WithError("err")

// 	err := s.strategy.Process(context.Background(), s.records)

// 	require.NoError(t, err)
// 	require.Equal(t, 3, s.spy.Count(EventType1))
// 	require.Equal(t, 3, s.spy.Count(EventType2))
// 	require.Equal(t, 3, s.spy.Count(EventType3))
// 	require.Equal(t, 0, s.spy.Count("err"))
// 	require.Equal(t, 3, s.spy.Count("h1"))
// }

// func TestPartitionError(t *testing.T) {
// 	s := setupPartition().
// 		WithHandler("h1", ModeError, EventType1, EventType2).
// 		WithPreHandler("pre1", ModeNormal).
// 		WithPostHandler("post1", ModeNormal).
// 		WithError("err")

// 	err := s.strategy.Process(context.Background(), s.records)

// 	require.Equal(t, appErr.ErrInternalError, err)
// 	require.Equal(t, 3, s.spy.Count(EventType1))
// 	require.Equal(t, 0, s.spy.Count(EventType2))
// 	require.Equal(t, 0, s.spy.Count(EventType3))
// 	require.Equal(t, 1, s.spy.Count("err"))
// 	require.Equal(t, 1, s.spy.Count("h1"))
// 	require.Equal(t, 1, s.spy.Count("pre1"))
// 	require.Equal(t, 0, s.spy.Count("post1"))
// }

// func TestPartitionPanic(t *testing.T) {
// 	t.Run("Panic Error", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModePanic, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err")

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.Equal(t, appErr.ErrPanic, err)
// 		require.Equal(t, 3, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 1, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 	})

// 	t.Run("Panic String", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModePanicString, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err")

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.Equal(t, appErr.ErrPanic, err)
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 1, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 	})
// }

// func TestPartitionPreError(t *testing.T) {
// 	s := setupPartition().
// 		WithHandler("h1", ModeNormal, EventType1, EventType2).
// 		WithPreHandler("pre1", ModeError).
// 		WithPreHandler("pre2", ModeError).
// 		WithPostHandler("post1", ModeNormal).
// 		WithError("err")

// 	err := s.strategy.Process(context.Background(), s.records)

// 	require.Equal(t, appErr.ErrInternalError, err)
// 	require.Equal(t, 1, s.spy.Count("err"))
// 	require.Equal(t, 0, s.spy.Count("h1"))
// 	require.Equal(t, 1, s.spy.Count("pre1"))
// 	require.Equal(t, 0, s.spy.Count("pre2"))
// 	require.Equal(t, 0, s.spy.Count("post1"))
// }

// func TestPartitionPrePanic(t *testing.T) {
// 	t.Run("Panic Error", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeNormal, EventType1, EventType2).
// 			WithPreHandler("pre1", ModePanic).
// 			WithPreHandler("pre2", ModePanic).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err")

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.Equal(t, appErr.ErrPanic, err)
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 0, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 	})

// 	t.Run("Panic String", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeNormal, EventType1, EventType2).
// 			WithPreHandler("pre1", ModePanicString).
// 			WithPreHandler("pre2", ModePanicString).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err")

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.Equal(t, appErr.ErrPanic, err)
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 0, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 	})
// }

// func TestPartitionPostError(t *testing.T) {
// 	s := setupPartition().
// 		WithHandler("h1", ModeNormal, EventType1, EventType2).
// 		WithPreHandler("pre1", ModeNormal).
// 		WithPostHandler("post1", ModeError).
// 		WithPostHandler("post2", ModeError).
// 		WithError("err")

// 	err := s.strategy.Process(context.Background(), s.records)

// 	require.Equal(t, appErr.ErrInternalError, err)
// 	require.Equal(t, 3, s.spy.Count(EventType1))
// 	require.Equal(t, 0, s.spy.Count(EventType2))
// 	require.Equal(t, 0, s.spy.Count(EventType3))
// 	require.Equal(t, 1, s.spy.Count("err"))
// 	require.Equal(t, 1, s.spy.Count("h1"))
// 	require.Equal(t, 1, s.spy.Count("pre1"))
// 	require.Equal(t, 1, s.spy.Count("post1"))
// 	require.Equal(t, 0, s.spy.Count("post2"))
// }

// func TestPartitionPostPanic(t *testing.T) {
// 	t.Run("Panic Error", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeNormal, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModePanic).
// 			WithPostHandler("post2", ModePanic).
// 			WithError("err")

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.Equal(t, appErr.ErrPanic, err)
// 		require.Equal(t, 3, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 1, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 1, s.spy.Count("post1"))
// 		require.Equal(t, 0, s.spy.Count("post2"))
// 	})

// 	t.Run("Panic String", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeNormal, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModePanicString).
// 			WithPostHandler("post2", ModePanicString).
// 			WithError("err")

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.Equal(t, appErr.ErrPanic, err)
// 		require.Equal(t, 3, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 1, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 1, s.spy.Count("post1"))
// 		require.Equal(t, 0, s.spy.Count("post2"))
// 	})
// }

// func TestPartitionDQL(t *testing.T) {
// 	t.Run("Retry 3", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeError, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err").
// 			WithDQL(3)

// 		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
// 			require.Len(t, s.dql.GetDQLErrors(), 3)
// 		}).Return(nil)

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.NoError(t, err)
// 		require.Equal(t, 9, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 3, s.spy.Count("err"))
// 		require.Equal(t, 3, s.spy.Count("h1"))
// 		require.Equal(t, 3, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 		s.dqlMock.AssertExpectations(t)
// 	})

// 	t.Run("Retry 1", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeError, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err").
// 			WithDQL(1)

// 		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
// 			require.Len(t, s.dql.GetDQLErrors(), 1)
// 		}).Return(nil)

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.NoError(t, err)
// 		require.Equal(t, 3, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 1, s.spy.Count("err"))
// 		require.Equal(t, 1, s.spy.Count("h1"))
// 		require.Equal(t, 1, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 		s.dqlMock.AssertExpectations(t)
// 	})

// 	t.Run("Retry 3 on Panic", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModePanic, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err").
// 			WithDQL(3)

// 		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
// 			require.Len(t, s.dql.GetDQLErrors(), 3)
// 		}).Return(nil)

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.NoError(t, err)
// 		require.Equal(t, 9, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 3, s.spy.Count("err"))
// 		require.Equal(t, 3, s.spy.Count("h1"))
// 		require.Equal(t, 3, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 		s.dqlMock.AssertExpectations(t)
// 	})

// 	t.Run("Retry 3 on Panic String", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModePanicString, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeNormal).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err").
// 			WithDQL(3)

// 		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
// 			require.Len(t, s.dql.GetDQLErrors(), 3)
// 		}).Return(nil)

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.NoError(t, err)
// 		require.Equal(t, 9, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 3, s.spy.Count("err"))
// 		require.Equal(t, 3, s.spy.Count("h1"))
// 		require.Equal(t, 3, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 		s.dqlMock.AssertExpectations(t)
// 	})
// }

// func TestPartitionPredDQL(t *testing.T) {
// 	t.Run("Retry 3", func(t *testing.T) {
// 		s := setupPartition().
// 			WithHandler("h1", ModeNormal, EventType1, EventType2).
// 			WithPreHandler("pre1", ModeError).
// 			WithPostHandler("post1", ModeNormal).
// 			WithError("err").
// 			WithDQL(3)

// 		s.dqlMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
// 			require.Len(t, s.dql.GetDQLErrors(), 3)
// 		}).Return(nil)

// 		err := s.strategy.Process(context.Background(), s.records)

// 		require.NoError(t, err)
// 		require.Equal(t, 0, s.spy.Count(EventType1))
// 		require.Equal(t, 0, s.spy.Count(EventType2))
// 		require.Equal(t, 0, s.spy.Count(EventType3))
// 		require.Equal(t, 3, s.spy.Count("err"))
// 		require.Equal(t, 0, s.spy.Count("h1"))
// 		require.Equal(t, 3, s.spy.Count("pre1"))
// 		require.Equal(t, 0, s.spy.Count("post1"))
// 		s.dqlMock.AssertExpectations(t)
// 	})
// }
