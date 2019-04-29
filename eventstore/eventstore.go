package eventstore

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/ddd"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/internal/common"
	"github.com/onedaycat/zamus/internal/common/clock"
	"github.com/onedaycat/zamus/internal/common/eid"
)

const emptyStr = ""

//go:generate mockery -name=EventStore
type EventStore interface {
	GetEvents(ctx context.Context, aggID string, seq int64) (event.Msgs, errors.Error)
	GetAggregate(ctx context.Context, aggID string, agg ddd.AggregateRoot) errors.Error
	GetAggregateBySeq(ctx context.Context, aggID string, agg ddd.AggregateRoot, seq int64) errors.Error
	Save(ctx context.Context, agg ddd.AggregateRoot) errors.Error
	SaveWithMetadata(ctx context.Context, agg ddd.AggregateRoot, metadata event.Metadata) errors.Error
	PublishEvents(ctx context.Context, evts ...event.Event) errors.Error
	PublishEventsWithMetadata(ctx context.Context, metadata map[string]string, evts ...event.Event) errors.Error
}

type eventStore struct {
	storage Storage
	opt     *option
}

func New(storage Storage, options ...Option) EventStore {
	es := &eventStore{
		storage: storage,
		opt:     &option{},
	}

	for _, o := range options {
		o(es.opt)
	}

	return es
}

func (es *eventStore) GetAggregateBySeq(ctx context.Context, aggID string, agg ddd.AggregateRoot, seq int64) errors.Error {
	msgs, err := es.storage.GetEvents(ctx, aggID, seq)
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		return nil
	}

	for _, msg := range msgs {
		agg.SetSequence(msg.Seq)
		seq = msg.Seq
		if err = agg.Apply(msg); err != nil {
			return err
		}
	}

	agg.SetAggregateID(aggID)

	return nil
}

func (es *eventStore) GetAggregate(ctx context.Context, aggID string, agg ddd.AggregateRoot) errors.Error {
	var seq int64

	if es.opt.allowSanpshot {
		snapshot, err := es.storage.GetSnapshot(ctx, aggID, es.opt.snapshotVersion)
		if err != nil {
			return err
		}

		if snapshot != nil {
			agg.SetAggregateID(snapshot.AggID)
			agg.SetSequence(snapshot.Seq)

			if err := common.UnmarshalJSONSnappy(snapshot.Agg, agg); err != nil {
				return err
			}

			seq = snapshot.Seq
		}
	}

	msgs, err := es.storage.GetEvents(ctx, aggID, seq)
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		return nil
	}

	for _, msg := range msgs {
		agg.SetSequence(msg.Seq)
		seq = msg.Seq
		if err = agg.Apply(msg); err != nil {
			return err
		}
	}

	agg.SetAggregateID(aggID)

	return nil
}

func (es *eventStore) GetEvents(ctx context.Context, id string, seq int64) (event.Msgs, errors.Error) {
	return es.storage.GetEvents(ctx, id, seq)
}

func (es *eventStore) GetSnapshot(ctx context.Context, aggID string, version int) (*Snapshot, errors.Error) {
	return es.storage.GetSnapshot(ctx, aggID, version)
}

func (es *eventStore) SaveWithMetadata(ctx context.Context, agg ddd.AggregateRoot, metadata event.Metadata) errors.Error {
	events := agg.GetEvents()
	n := len(events)
	if n == 0 {
		return nil
	}

	if agg.GetAggregateID() == emptyStr {
		return appErr.ErrNoAggregateID.WithCaller().WithInput(agg)
	}

	msgs := make(event.Msgs, n)
	now := clock.Now().Unix()
	eventTypes := agg.GetEventTypes()

	var lastEvent *event.Msg

	for i := 0; i < n; i++ {
		agg.IncreaseSequence()
		aggid := agg.GetAggregateID()
		seq := agg.GetSequence()
		evtID := eid.GenerateID()

		eventAny, err := event.MarshalEvent(events[i])
		if err != nil {
			return err
		}

		msgs[i] = &event.Msg{
			Id:        evtID,
			EventType: eventTypes[i],
			AggID:     aggid,
			Seq:       seq,
			Event:     eventAny,
			Time:      now,
			Metadata:  metadata,
		}
	}

	lastEvent = msgs[len(msgs)-1]

	if lastEvent.Seq == 0 {
		return appErr.ErrInvalidVersionNotAllowed.WithCaller().WithInput(agg)
	}

	var snapshot *Snapshot

	if es.opt.allowSanpshot {
		aggDataSnap, err := common.MarshalJSONSnappy(agg)
		if err != nil {
			return err
		}

		snapshot = &Snapshot{
			AggID:      agg.GetAggregateID(),
			Agg:        aggDataSnap,
			EventMsgID: lastEvent.Id,
			Time:       lastEvent.Time,
			Seq:        lastEvent.Seq,
			Version:    es.opt.snapshotVersion,
		}
	}

	if err := es.storage.Save(ctx, msgs, snapshot); err != nil {
		return err
	}

	agg.ClearEvents()

	return nil
}

func (es *eventStore) Save(ctx context.Context, agg ddd.AggregateRoot) errors.Error {
	return es.SaveWithMetadata(ctx, agg, nil)
}

func (es *eventStore) PublishEventsWithMetadata(ctx context.Context, metadata map[string]string, evts ...event.Event) errors.Error {
	msgs := make(event.Msgs, len(evts))
	now := clock.Now().Unix()
	var seq int64
	seq = 1

	for i := 0; i < len(evts); i++ {
		aggid := eid.GenerateID()
		evtid := aggid
		eventType := event.EventType(evts[i])
		eventPayload, err := event.MarshalEvent(evts[i])
		if err != nil {
			return err
		}

		msgs[i] = &event.Msg{
			Id:        evtid,
			EventType: eventType,
			AggID:     aggid,
			Event:     eventPayload,
			Time:      now,
			Seq:       seq,
			Metadata:  metadata,
		}
		seq++
	}

	return es.storage.Save(ctx, msgs, nil)
}

func (es *eventStore) PublishEvents(ctx context.Context, evts ...event.Event) errors.Error {
	return es.PublishEventsWithMetadata(ctx, nil, evts...)
}
