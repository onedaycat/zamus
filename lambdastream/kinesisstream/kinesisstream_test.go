package kinesisstream_test

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/dql/mocks"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
)

const (
	MODE_NORMAL = iota
	MODE_PANIC
	MODE_PANIC_STRING
	MODE_ERROR
)

const (
	AGG_1        = "AGG_1"
	AGG_2        = "AGG_2"
	AGG_3        = "AGG_3"
	EVENT_TYPE_1 = "EVENT_TYPE_1"
	EVENT_TYPE_2 = "EVENT_TYPE_2"
	EVENT_TYPE_3 = "EVENT_TYPE_3"
)

var _records kinesisstream.Records

type StrategySuite struct {
	strategy kinesisstream.KinesisHandlerStrategy
	spy      *common.SpyTest
	records  kinesisstream.Records
	dqlMock  *mocks.Storage
	dql      dql.DQL
}

func setupShard() *StrategySuite {
	return setupSuite(kinesisstream.NewShardStrategy(1))
}

func setupMultiShard() *StrategySuite {
	return setupSuite(kinesisstream.NewShardStrategy(3))
}

func setupPartition() *StrategySuite {
	return setupSuite(kinesisstream.NewPartitionStrategy())
}

func setupSimple() *StrategySuite {
	return setupSuite(kinesisstream.NewSimpleStrategy())
}

func setupSuite(strategy kinesisstream.KinesisHandlerStrategy) *StrategySuite {
	s := &StrategySuite{
		spy:     common.Spy(),
		dqlMock: &mocks.Storage{},
	}

	s.strategy = strategy

	if _records == nil {
		e1 := random.EventMsgs().RandomEventMsgs(3,
			random.WithAggregateID(AGG_1),
			random.WithEventType(EVENT_TYPE_1),
		).Build()

		e2 := random.EventMsgs().RandomEventMsgs(3,
			random.WithAggregateID(AGG_2),
			random.WithEventType(EVENT_TYPE_2),
		).Build()

		e3 := random.EventMsgs().RandomEventMsgs(3,
			random.WithAggregateID(AGG_3),
			random.WithEventType(EVENT_TYPE_3),
		).Build()

		_records = random.KinesisEvents().Add(e1...).Add(e2...).Add(e3...).Build().Records
	}

	s.records = _records

	return s
}

func (s *StrategySuite) WithPreHandler(name string, mode int) *StrategySuite {
	s.strategy.PreHandlers(func(ctx context.Context, msgs kinesisstream.EventMsgs) errors.Error {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return appErr.ErrInternalError
		}

		return nil
	})

	return s
}

func (s *StrategySuite) WithPostHandler(name string, mode int) *StrategySuite {
	s.strategy.PostHandlers(func(ctx context.Context, msgs kinesisstream.EventMsgs) errors.Error {
		s.spy.Called(name)

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return appErr.ErrInternalError
		}

		return nil
	})

	return s
}

func (s *StrategySuite) WithHandler(name string, mode int, eventTypes ...string) *StrategySuite {
	s.strategy.RegisterHandler(func(ctx context.Context, msgs kinesisstream.EventMsgs) errors.Error {
		s.spy.Called(name)

		for _, msg := range msgs {
			s.spy.Called(msg.EventType)
			s.spy.Called(msg.AggregateID)
		}

		switch mode {
		case MODE_PANIC:
			panic(appErr.ErrInternalError)
		case MODE_PANIC_STRING:
			panic("string")
		case MODE_ERROR:
			return appErr.ErrInternalError
		}

		return nil
	}, eventTypes)

	return s
}

func (s *StrategySuite) WithError(name string) *StrategySuite {
	s.strategy.ErrorHandlers(func(ctx context.Context, msgs kinesisstream.EventMsgs, err errors.Error) {
		s.spy.Called(name)
	})

	return s
}

func (s *StrategySuite) WithDQL(retry int) *StrategySuite {
	s.dql = dql.New(s.dqlMock, retry, "service", "lambdaFunc", "1.0.0")
	s.strategy.SetDQL(s.dql)

	return s
}
