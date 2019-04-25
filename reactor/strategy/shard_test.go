package strategy_test

import (
    "context"
    "testing"

    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"

    appErr "github.com/onedaycat/zamus/errors"
)

func TestShardHandler(t *testing.T) {
    s := setupShard().
        WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[2]).
        WithHandler("h2", ModeNormal, EventTypes[2]).
        WithError("err")

    err := s.strategy.Process(context.Background(), s.records)

    require.NoError(t, err)
    require.Equal(t, 3, s.spy.Count(EventTypes[0]))
    require.Equal(t, 0, s.spy.Count(EventTypes[1]))
    require.Equal(t, 6, s.spy.Count(EventTypes[2]))
    require.Equal(t, 0, s.spy.Count("err"))
    require.Equal(t, 2, s.spy.Count("h1"))
    require.Equal(t, 1, s.spy.Count("h2"))
}

func TestShardHandlerWithPreAndPost(t *testing.T) {
    s := setupShard().
        WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[2]).
        WithHandler("h2", ModeNormal, EventTypes[2]).
        WithPreHandler("pre1", ModeNormal).
        WithPreHandler("pre2", ModeNormal).
        WithPostHandler("post1", ModeNormal).
        WithPostHandler("post2", ModeNormal).
        WithError("err")

    err := s.strategy.Process(context.Background(), s.records)

    require.NoError(t, err)
    require.Equal(t, 3, s.spy.Count(EventTypes[0]))
    require.Equal(t, 0, s.spy.Count(EventTypes[1]))
    require.Equal(t, 6, s.spy.Count(EventTypes[2]))
    require.Equal(t, 0, s.spy.Count("err"))
    require.Equal(t, 2, s.spy.Count("h1"))
    require.Equal(t, 1, s.spy.Count("h2"))
    require.Equal(t, 3, s.spy.Count("pre1"))
    require.Equal(t, 3, s.spy.Count("pre2"))
    require.Equal(t, 3, s.spy.Count("post1"))
    require.Equal(t, 3, s.spy.Count("post2"))
}

func TestShardHandlerNoFilter(t *testing.T) {
    s := setupShard().
        WithHandler("h1", ModeNormal).
        WithError("err")

    err := s.strategy.Process(context.Background(), s.records)

    require.NoError(t, err)
    require.Equal(t, 3, s.spy.Count(EventTypes[0]))
    require.Equal(t, 3, s.spy.Count(EventTypes[1]))
    require.Equal(t, 3, s.spy.Count(EventTypes[2]))
    require.Equal(t, 0, s.spy.Count("err"))
    require.Equal(t, 3, s.spy.Count("h1"))
}

func TestShardError(t *testing.T) {
    s := setupShard().
        WithHandler("h1", ModeError, EventTypes[0], EventTypes[1]).
        WithPreHandler("pre1", ModeNormal).
        WithPostHandler("post1", ModeNormal).
        WithError("err")

    err := s.strategy.Process(context.Background(), s.records)

    require.Equal(t, appErr.ErrInternalError, err)
    require.Equal(t, 3, s.spy.Count(EventTypes[0]))
    require.Equal(t, 0, s.spy.Count(EventTypes[1]))
    require.Equal(t, 0, s.spy.Count(EventTypes[2]))
    require.Equal(t, 1, s.spy.Count("err"))
    require.Equal(t, 1, s.spy.Count("h1"))
    require.Equal(t, 1, s.spy.Count("pre1"))
    require.Equal(t, 0, s.spy.Count("post1"))
}

func TestShardPanic(t *testing.T) {
    t.Run("Panic Error", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModePanic, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModeNormal).
            WithError("err")

        err := s.strategy.Process(context.Background(), s.records)

        require.Equal(t, appErr.ErrPanic, err)
        require.Equal(t, 3, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 1, s.spy.Count("err"))
        require.Equal(t, 1, s.spy.Count("h1"))
        require.Equal(t, 1, s.spy.Count("pre1"))
        require.Equal(t, 0, s.spy.Count("post1"))
    })

    t.Run("Panic String", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModePanicString, EventTypes[0], EventTypes[1]).
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

func TestShardPreError(t *testing.T) {
    s := setupShard().
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

func TestShardPrePanic(t *testing.T) {
    t.Run("Panic Error", func(t *testing.T) {
        s := setupShard().
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
        s := setupShard().
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

func TestShardPostError(t *testing.T) {
    s := setupShard().
        WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
        WithPreHandler("pre1", ModeNormal).
        WithPostHandler("post1", ModeError).
        WithPostHandler("post2", ModeError).
        WithError("err")

    err := s.strategy.Process(context.Background(), s.records)

    require.Equal(t, appErr.ErrInternalError, err)
    require.Equal(t, 3, s.spy.Count(EventTypes[0]))
    require.Equal(t, 0, s.spy.Count(EventTypes[1]))
    require.Equal(t, 0, s.spy.Count(EventTypes[2]))
    require.Equal(t, 1, s.spy.Count("err"))
    require.Equal(t, 1, s.spy.Count("h1"))
    require.Equal(t, 1, s.spy.Count("pre1"))
    require.Equal(t, 1, s.spy.Count("post1"))
    require.Equal(t, 0, s.spy.Count("post2"))
}

func TestShardPostPanic(t *testing.T) {
    t.Run("Panic Error", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModePanic).
            WithPostHandler("post2", ModePanic).
            WithError("err")

        err := s.strategy.Process(context.Background(), s.records)

        require.Equal(t, appErr.ErrPanic, err)
        require.Equal(t, 3, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 1, s.spy.Count("err"))
        require.Equal(t, 1, s.spy.Count("h1"))
        require.Equal(t, 1, s.spy.Count("pre1"))
        require.Equal(t, 1, s.spy.Count("post1"))
        require.Equal(t, 0, s.spy.Count("post2"))
    })

    t.Run("Panic String", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModePanicString).
            WithPostHandler("post2", ModePanicString).
            WithError("err")

        err := s.strategy.Process(context.Background(), s.records)

        require.Equal(t, appErr.ErrPanic, err)
        require.Equal(t, 3, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 1, s.spy.Count("err"))
        require.Equal(t, 1, s.spy.Count("h1"))
        require.Equal(t, 1, s.spy.Count("pre1"))
        require.Equal(t, 1, s.spy.Count("post1"))
        require.Equal(t, 0, s.spy.Count("post2"))
    })
}

func TestShardDLQ(t *testing.T) {
    t.Run("Retry 3", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModeError, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModeNormal).
            WithError("err").
            WithDLQ(3)

        s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
            require.Len(t, s.dlq.GetDLQErrors(), 3)
        }).Return(nil)

        err := s.strategy.Process(context.Background(), s.records)

        require.NoError(t, err)
        require.Equal(t, 9, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 3, s.spy.Count("err"))
        require.Equal(t, 3, s.spy.Count("h1"))
        require.Equal(t, 3, s.spy.Count("pre1"))
        require.Equal(t, 0, s.spy.Count("post1"))
        s.dlqMock.AssertExpectations(t)
    })

    t.Run("Retry 1", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModeError, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModeNormal).
            WithError("err").
            WithDLQ(1)

        s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
            require.Len(t, s.dlq.GetDLQErrors(), 1)
        }).Return(nil)

        err := s.strategy.Process(context.Background(), s.records)

        require.NoError(t, err)
        require.Equal(t, 3, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 1, s.spy.Count("err"))
        require.Equal(t, 1, s.spy.Count("h1"))
        require.Equal(t, 1, s.spy.Count("pre1"))
        require.Equal(t, 0, s.spy.Count("post1"))
        s.dlqMock.AssertExpectations(t)
    })

    t.Run("Retry 3 on Panic", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModePanic, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModeNormal).
            WithError("err").
            WithDLQ(3)

        s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
            require.Len(t, s.dlq.GetDLQErrors(), 3)
        }).Return(nil)

        err := s.strategy.Process(context.Background(), s.records)

        require.NoError(t, err)
        require.Equal(t, 9, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 3, s.spy.Count("err"))
        require.Equal(t, 3, s.spy.Count("h1"))
        require.Equal(t, 3, s.spy.Count("pre1"))
        require.Equal(t, 0, s.spy.Count("post1"))
        s.dlqMock.AssertExpectations(t)
    })

    t.Run("Retry 3 on Panic String", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModePanicString, EventTypes[0], EventTypes[1]).
            WithPreHandler("pre1", ModeNormal).
            WithPostHandler("post1", ModeNormal).
            WithError("err").
            WithDLQ(3)

        s.dlqMock.On("Save", mock.Anything, mock.Anything).Run(func(ars mock.Arguments) {
            require.Len(t, s.dlq.GetDLQErrors(), 3)
        }).Return(nil)

        err := s.strategy.Process(context.Background(), s.records)

        require.NoError(t, err)
        require.Equal(t, 9, s.spy.Count(EventTypes[0]))
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 3, s.spy.Count("err"))
        require.Equal(t, 3, s.spy.Count("h1"))
        require.Equal(t, 3, s.spy.Count("pre1"))
        require.Equal(t, 0, s.spy.Count("post1"))
        s.dlqMock.AssertExpectations(t)
    })
}

func TestShardPredDLQ(t *testing.T) {
    t.Run("Retry 3", func(t *testing.T) {
        s := setupShard().
            WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[1]).
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
        require.Equal(t, 0, s.spy.Count(EventTypes[1]))
        require.Equal(t, 0, s.spy.Count(EventTypes[2]))
        require.Equal(t, 3, s.spy.Count("err"))
        require.Equal(t, 0, s.spy.Count("h1"))
        require.Equal(t, 3, s.spy.Count("pre1"))
        require.Equal(t, 0, s.spy.Count("post1"))
        s.dlqMock.AssertExpectations(t)
    })
}

func TestMultiShardHandlerWithPreAndPost(t *testing.T) {
    s := setupMultiShard().
        WithHandler("h1", ModeNormal, EventTypes[0], EventTypes[2]).
        WithHandler("h2", ModeNormal, EventTypes[2]).
        WithPreHandler("pre1", ModeNormal).
        WithPreHandler("pre2", ModeNormal).
        WithPostHandler("post1", ModeNormal).
        WithPostHandler("post2", ModeNormal).
        WithError("err")

    err := s.strategy.Process(context.Background(), s.records)

    require.NoError(t, err)
    require.Equal(t, 3, s.spy.Count(EventTypes[0]))
    require.Equal(t, 0, s.spy.Count(EventTypes[1]))
    require.Equal(t, 6, s.spy.Count(EventTypes[2]))
    require.Equal(t, 0, s.spy.Count("err"))
    require.Equal(t, 6, s.spy.Count("h1"))
    require.Equal(t, 3, s.spy.Count("h2"))
    require.Equal(t, 9, s.spy.Count("pre1"))
    require.Equal(t, 9, s.spy.Count("pre2"))
    require.Equal(t, 9, s.spy.Count("post1"))
    require.Equal(t, 9, s.spy.Count("post2"))
}
