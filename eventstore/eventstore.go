package eventstore

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/ddd"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common/clock"
	"github.com/onedaycat/zamus/internal/common/eid"
)

const emptyStr = ""

//go:generate mockery -name=EventStore
type EventStore interface {
	Save(ctx context.Context, agg ddd.AggregateRoot) errors.Error
	PublishEvents(ctx context.Context, evts ...event.Event) errors.Error
}

type eventStore struct {
	storage Storage
}

func New(storage Storage) EventStore {
	es := &eventStore{
		storage: storage,
	}

	return es
}

func (es *eventStore) Save(ctx context.Context, agg ddd.AggregateRoot) errors.Error {
	events := agg.GetEvents()
	n := len(events)
	if n == 0 {
		return nil
	}

	if agg.GetAggregateID() == emptyStr {
		return appErr.ErrNoAggregateID.WithCaller().WithInput(agg)
	}

	meta, _ := event.MetadataFromContext(ctx)
	msgs := make(event.Msgs, n)
	now := clock.Now()
	eventTypes := agg.GetEventTypes()
	aggid := agg.GetAggregateID()

	for i := 0; i < n; i++ {
		evtID := eid.GenerateID()

		eventAny, err := event.MarshalEvent(events[i])
		if err != nil {
			return err
		}

		msgs[i] = &event.Msg{
			Id:        evtID,
			AggID:     aggid,
			EventType: eventTypes[i],
			Event:     eventAny,
			Time:      now.Unix(),
			Seq:       now.UnixNano(),
			Metadata:  meta,
		}
	}

	if err := es.storage.Save(ctx, msgs); err != nil {
		return err
	}

	agg.ClearEvents()

	return nil
}

func (es *eventStore) PublishEvents(ctx context.Context, evts ...event.Event) errors.Error {
	msgs := make(event.Msgs, len(evts))
	now := clock.Now()

	meta, _ := event.MetadataFromContext(ctx)

	for i := 0; i < len(evts); i++ {
		evtid := eid.GenerateID()
		eventType := event.EventType(evts[i])
		eventPayload, err := event.MarshalEvent(evts[i])
		if err != nil {
			return err
		}

		msgs[i] = &event.Msg{
			Id:        evtid,
			AggID:     eventType,
			EventType: eventType,
			Event:     eventPayload,
			Time:      now.Unix(),
			Seq:       now.UnixNano(),
			Metadata:  meta,
		}
	}

	return es.storage.Save(ctx, msgs)
}
