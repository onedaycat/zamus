package eventstore

import (
	"context"
	"encoding/json"

	"github.com/golang/snappy"
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	"github.com/onedaycat/zamus/errors"
)

//go:generate mockery -name=EventStore

// RetryHandler if return bool is true is allow retry,
// if return bool is false no retry
type RetryHandler func() error

const emptyStr = ""

type EventStore interface {
	SetSnapshotEveryNEvents(n int64)
	GetEvents(ctx context.Context, aggID string, timeSeq int64) ([]*EventMsg, error)
	GetAggregate(ctx context.Context, aggID string, agg AggregateRoot) error
	GetAggregateByTimeSeq(ctx context.Context, aggID string, agg AggregateRoot, timeSeq int64) error
	Save(ctx context.Context, agg AggregateRoot) error
	SaveWithMetadata(ctx context.Context, agg AggregateRoot, metadata *Metadata) error
}

type eventStore struct {
	storage   Storage
	snapshotn int64
}

func NewEventStore(storage Storage) EventStore {
	return &eventStore{
		storage:   storage,
		snapshotn: 1,
	}
}

func (es *eventStore) SetSnapshotEveryNEvents(n int64) {
	if n < 1 {
		es.snapshotn = 1
	}
	es.snapshotn = n
}

func (es *eventStore) GetAggregateByTimeSeq(ctx context.Context, aggID string, agg AggregateRoot, timeSeq int64) error {
	msgs, err := es.storage.GetEvents(ctx, aggID, timeSeq, 0)
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		return errors.ErrNotFound
	}

	agg.SetAggregateID(aggID)

	for _, msg := range msgs {
		agg.SetSequence(msg.Seq)
		agg.SetLastEventID(msg.EventID)
		agg.SetLastEventTime(msg.Time)
		if err = agg.Apply(msg); err != nil {
			return errors.Warp(err).WithCaller().WithInput(agg)
		}
	}

	return nil
}

func (es *eventStore) GetAggregate(ctx context.Context, aggID string, agg AggregateRoot) error {
	snapshot, err := es.storage.GetSnapshot(ctx, aggID)
	if err != nil {
		return err
	}

	agg.SetAggregateID(snapshot.AggregateID)
	agg.SetSequence(snapshot.Seq)
	agg.SetLastEventID(snapshot.EventID)
	agg.SetLastEventTime(snapshot.Time)

	var dst []byte
	dst, err = snappy.Decode(dst, snapshot.Aggregate)
	if err != nil {
		return errors.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(aggID)
	}

	if err = json.Unmarshal(dst, agg); err != nil {
		return errors.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(aggID)
	}

	if es.snapshotn == 1 {
		return nil
	}

	msgs, err := es.storage.GetEvents(ctx, aggID, snapshot.TimeSeq, 0)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		agg.SetSequence(msg.Seq)
		agg.SetLastEventID(msg.EventID)
		agg.SetLastEventTime(msg.Time)
		if err = agg.Apply(msg); err != nil {
			return errors.Warp(err).WithCaller().WithInput(agg)
		}
	}

	return nil
}

func (es *eventStore) GetEvents(ctx context.Context, id string, timeSeq int64) ([]*EventMsg, error) {
	return es.storage.GetEvents(ctx, id, timeSeq, 0)
}

func (es *eventStore) GetSnapshot(ctx context.Context, aggID string) (*Snapshot, error) {
	return es.storage.GetSnapshot(ctx, aggID)
}

func (es *eventStore) SaveWithMetadata(ctx context.Context, agg AggregateRoot, metadata *Metadata) error {
	events := agg.GetEvents()
	n := len(events)
	if n == 0 {
		return nil
	}

	if n > 10 {
		return errors.ErrEventLimitExceed.WithCaller().WithInput(agg)
	}

	if agg.GetAggregateID() == emptyStr {
		return errors.ErrNoAggregateID.WithCaller().WithInput(agg)
	}

	msgs := make([]*EventMsg, n)
	now := clock.Now().Unix()
	eventTypes := agg.GetEventTypes()

	var lastEvent *EventMsg
	var saveSnapshot bool

	var metadataByte []byte
	if metadata != nil {
		metadataByte, _ = metadata.Marshal()
	}

	for i := 0; i < n; i++ {
		agg.IncreaseSequence()
		aggid := agg.GetAggregateID()
		seq := agg.GetSequence()
		eid := eid.CreateEventID(aggid, seq)
		eventData, err := json.Marshal(events[i])
		if err != nil {
			return errors.ErrUnbleSaveEventStore.WithCause(err).WithCaller().WithInput(agg)
		}

		if !saveSnapshot && seq%es.snapshotn == 0 {
			saveSnapshot = true
		}

		var eventDataSnap []byte
		eventDataSnap = snappy.Encode(eventDataSnap, eventData)

		msgs[i] = &EventMsg{
			EventID:     eid,
			EventType:   eventTypes[i],
			AggregateID: aggid,
			Seq:         seq,
			Event:       eventDataSnap,
			Time:        now,
			TimeSeq:     TimeSeq(now, seq),
			Metadata:    metadataByte,
		}
	}

	lastEvent = msgs[len(msgs)-1]

	if lastEvent.Seq == 0 {
		return errors.ErrInvalidVersionNotAllowed.WithCaller().WithInput(agg)
	}

	var snapshot *Snapshot
	if saveSnapshot {
		aggData, err := json.Marshal(agg)
		if err != nil {
			return errors.ErrUnbleSaveEventStore.WithCause(err).WithCaller().WithInput(agg)
		}

		var aggDataSnap []byte
		aggDataSnap = snappy.Encode(aggDataSnap, aggData)

		snapshot = &Snapshot{
			AggregateID: agg.GetAggregateID(),
			Aggregate:   aggDataSnap,
			EventID:     lastEvent.EventID,
			Time:        lastEvent.Time,
			Seq:         lastEvent.Seq,
			TimeSeq:     lastEvent.TimeSeq,
		}
	}

	if err := es.storage.Save(ctx, msgs, snapshot); err != nil {
		return err
	}

	agg.ClearEvents()
	agg.SetLastEventTime(lastEvent.Time)
	agg.SetLastEventID(lastEvent.EventID)

	return nil
}

func (es *eventStore) Save(ctx context.Context, agg AggregateRoot) error {
	return es.SaveWithMetadata(ctx, agg, nil)
}

func TimeSeq(time int64, seq int64) int64 {
	if time < 0 {
		return 0
	}

	if seq < 0 {
		seq = 0
	}

	return (time * 100000) + seq
}
