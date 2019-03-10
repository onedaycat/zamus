package eventstore

import (
	"context"
	"encoding/json"

	"github.com/golang/snappy"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
	appErr "github.com/onedaycat/zamus/errors"
)

//go:generate mockery -name=EventStore

const emptyStr = ""

type EventStore interface {
	GetEvents(ctx context.Context, aggID string, seq int64) ([]*EventMsg, errors.Error)
	GetAggregate(ctx context.Context, aggID string, agg AggregateRoot) errors.Error
	GetAggregateBySeq(ctx context.Context, aggID string, agg AggregateRoot, seq int64) errors.Error
	Save(ctx context.Context, agg AggregateRoot) errors.Error
	SaveWithMetadata(ctx context.Context, agg AggregateRoot, metadata *Metadata) errors.Error
}

type eventStore struct {
	storage Storage
}

func NewEventStore(storage Storage) EventStore {
	return &eventStore{
		storage: storage,
	}
}

func (es *eventStore) GetAggregateBySeq(ctx context.Context, aggID string, agg AggregateRoot, seq int64) errors.Error {
	msgs, err := es.storage.GetEvents(ctx, aggID, seq)
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		return nil
	}

	for _, msg := range msgs {
		agg.SetSequence(msg.Seq)
		agg.SetLastEventID(msg.EventID)
		agg.SetLastEventTime(msg.Time)
		seq = msg.Seq
		if err = agg.Apply(msg); err != nil {
			return err
		}
	}

	agg.SetAggregateID(aggID)

	return nil
}

func (es *eventStore) GetAggregate(ctx context.Context, aggID string, agg AggregateRoot) errors.Error {
	var seq int64

	snapshot, err := es.storage.GetSnapshot(ctx, aggID, agg.SnapshotVersion())
	if err != nil {
		return err
	}

	if snapshot != nil {
		agg.SetAggregateID(snapshot.AggregateID)
		agg.SetSequence(snapshot.Seq)
		agg.SetLastEventID(snapshot.EventID)
		agg.SetLastEventTime(snapshot.Time)

		var dst []byte
		var serr error
		dst, serr = snappy.Decode(dst, snapshot.Aggregate)
		if serr != nil {
			return appErr.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(aggID)
		}

		if serr = json.Unmarshal(dst, agg); err != nil {
			return appErr.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(aggID)
		}

		seq = snapshot.Seq
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
		agg.SetLastEventID(msg.EventID)
		agg.SetLastEventTime(msg.Time)
		seq = msg.Seq
		if err = agg.Apply(msg); err != nil {
			return err
		}
	}

	agg.SetAggregateID(aggID)

	return nil
}

func (es *eventStore) GetEvents(ctx context.Context, id string, seq int64) ([]*EventMsg, errors.Error) {
	return es.storage.GetEvents(ctx, id, seq)
}

func (es *eventStore) GetSnapshot(ctx context.Context, aggID string, version int) (*Snapshot, errors.Error) {
	return es.storage.GetSnapshot(ctx, aggID, version)
}

func (es *eventStore) SaveWithMetadata(ctx context.Context, agg AggregateRoot, metadata *Metadata) errors.Error {
	events := agg.GetEvents()
	n := len(events)
	if n == 0 {
		return nil
	}

	if agg.GetAggregateID() == emptyStr {
		return appErr.ErrNoAggregateID.WithCaller().WithInput(agg)
	}

	msgs := make([]*EventMsg, n)
	now := clock.Now().Unix()
	eventTypes := agg.GetEventTypes()

	var lastEvent *EventMsg

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
			return appErr.ErrUnbleSaveEventStore.WithCause(err).WithCaller().WithInput(agg)
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
			Metadata:    metadataByte,
		}
	}

	lastEvent = msgs[len(msgs)-1]

	if lastEvent.Seq == 0 {
		return appErr.ErrInvalidVersionNotAllowed.WithCaller().WithInput(agg)
	}

	var snapshot *Snapshot
	aggData, err := json.Marshal(agg)
	if err != nil {
		return appErr.ErrUnbleSaveEventStore.WithCause(err).WithCaller().WithInput(agg)
	}

	var aggDataSnap []byte
	aggDataSnap = snappy.Encode(aggDataSnap, aggData)

	snapshot = &Snapshot{
		AggregateID: agg.GetAggregateID(),
		Aggregate:   aggDataSnap,
		EventID:     lastEvent.EventID,
		Time:        lastEvent.Time,
		Seq:         lastEvent.Seq,
		Version:     agg.CurrentVersion(),
	}

	if err := es.storage.Save(ctx, msgs, snapshot); err != nil {
		return err
	}

	agg.ClearEvents()
	agg.SetLastEventTime(lastEvent.Time)
	agg.SetLastEventID(lastEvent.EventID)

	return nil
}

func (es *eventStore) Save(ctx context.Context, agg AggregateRoot) errors.Error {
	return es.SaveWithMetadata(ctx, agg, nil)
}
