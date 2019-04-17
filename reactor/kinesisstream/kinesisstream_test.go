package kinesisstream_test

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dql"
    "github.com/onedaycat/zamus/dql/mocks"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/random"
    "github.com/onedaycat/zamus/reactor/kinesisstream"
    "github.com/onedaycat/zamus/testdata/domain"
)

const (
    ModeNormal = iota
    ModePanic
    ModePanicString
    ModeError
)

const (
    Agg1 = "Agg1"
    Agg2 = "Agg2"
    Agg3 = "Agg3"
)

var EventTypes = event.EventTypes((*domain.StockItemCreated)(nil), (*domain.StockItemUpdated)(nil), (*domain.StockItemRemoved)(nil))

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

//noinspection GoUnusedFunction
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
            random.WithAggregateID(Agg1),
            random.WithEvent(&domain.StockItemCreated{}),
        ).Build()

        e2 := random.EventMsgs().RandomEventMsgs(3,
            random.WithAggregateID(Agg2),
            random.WithEvent(&domain.StockItemUpdated{}),
        ).Build()

        e3 := random.EventMsgs().RandomEventMsgs(3,
            random.WithAggregateID(Agg3),
            random.WithEvent(&domain.StockItemRemoved{}),
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
        case ModePanic:
            panic(appErr.ErrInternalError)
        case ModePanicString:
            panic("string")
        case ModeError:
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
        case ModePanic:
            panic(appErr.ErrInternalError)
        case ModePanicString:
            panic("string")
        case ModeError:
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
            s.spy.Called(msg.AggID)
        }

        switch mode {
        case ModePanic:
            panic(appErr.ErrInternalError)
        case ModePanicString:
            panic("string")
        case ModeError:
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
