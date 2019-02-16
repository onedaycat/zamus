package gocqrs

import (
	"encoding/json"
	"time"

	"github.com/onedaycat/gocqrs/common/clock"
	"github.com/onedaycat/gocqrs/common/eid"
)

//go:generate mockery -name=EventStore
//go:generate protoc --gofast_out=. event.proto

// RetryHandler if return bool is true is allow retry,
// if return bool is false no retry
type RetryHandler func() error

const emptyStr = ""

type EventType = string

type EventStore interface {
	SetEventLimit(limit int64)
	SetSnapshotStrategy(strategies SnapshotStategyHandler)
	GetEvents(aggID string, seq int64, agg AggregateRoot) ([]*EventMessage, error)
	GetEventsByEventType(eventType EventType, seq int64) ([]*EventMessage, error)
	GetEventsByAggregateType(aggType AggregateType, seq int64) ([]*EventMessage, error)
	GetAggregate(aggID string, agg AggregateRoot) error
	GetSnapshot(aggID string, agg AggregateRoot) error
	Save(agg AggregateRoot) error
}

type eventStore struct {
	storage          Storage
	limit            int64
	snapshotStrategy SnapshotStategyHandler
}

func NewEventStore(storage Storage) EventStore {
	return &eventStore{
		storage:          storage,
		limit:            100,
		snapshotStrategy: LatestEventSanpshot(),
	}
}

func (es *eventStore) SetEventLimit(limit int64) {
	es.limit = limit
}

func (es *eventStore) SetSnapshotStrategy(strategies SnapshotStategyHandler) {
	es.snapshotStrategy = strategies
}

func (es *eventStore) GetAggregate(id string, agg AggregateRoot) error {
	err := es.GetSnapshot(id, agg)
	if err != nil && err != ErrNotFound {
		return err
	}

	hashKey := es.createHashKey(id, agg.GetAggregateType())

	return es.getAggregateFromEvent(id, hashKey, agg, agg.GetSequence())
}

func (es *eventStore) getAggregateFromEvent(id, hashKey string, agg AggregateRoot, seq int64) error {
	events, err := es.storage.GetEvents(id, hashKey, seq, es.limit)
	if err != nil {
		return err
	}

	n := len(events)

	if n == 0 {
		return ErrNotFound
	}

	lastEvent := events[n-1]

	agg.SetAggregateID(lastEvent.AggregateID)
	agg.SetSequence(lastEvent.Seq)

	for _, event := range events {
		if err = agg.Apply(event); err != nil {
			return err
		}
	}

	for n >= int(es.limit) {
		if err = es.getAggregateFromEvent(id, hashKey, agg, lastEvent.Seq); err != nil {
			if err == ErrNotFound {
				break
			}
			return err
		}
	}

	if agg.IsNew() {
		return ErrNotFound
	}

	return nil
}

func (es *eventStore) GetEvents(id string, seq int64, agg AggregateRoot) ([]*EventMessage, error) {
	hashKey := es.createHashKey(id, agg.GetAggregateType())
	return es.storage.GetEvents(id, hashKey, seq, es.limit)
}

func (es *eventStore) GetEventsByEventType(eventType EventType, seq int64) ([]*EventMessage, error) {
	return es.storage.GetEventsByEventType(eventType, seq, es.limit)
}

func (es *eventStore) GetEventsByAggregateType(aggType AggregateType, seq int64) ([]*EventMessage, error) {
	return es.storage.GetEventsByAggregateType(aggType, seq, es.limit)
}

func (es *eventStore) GetSnapshot(id string, agg AggregateRoot) error {
	hashKey := es.createHashKey(id, agg.GetAggregateType())
	snapshot, err := es.storage.GetSnapshot(id, hashKey)
	if err != nil {
		return err
	}

	agg.SetAggregateID(snapshot.AggregateID)
	agg.SetSequence(snapshot.Seq)

	if err = json.Unmarshal(snapshot.Payload, agg); err != nil {
		return err
	}

	return nil
}

func (es *eventStore) Save(agg AggregateRoot) error {
	payloads := agg.GetEventPayloads()
	n := len(payloads)
	if n == 0 {
		return nil
	}

	if n > 9 {
		return ErrEventLimitExceed
	}

	if agg.GetAggregateID() == emptyStr {
		return ErrNoAggregateID
	}

	events := make([]*EventMessage, n)
	now := clock.Now().Unix()
	aggType := agg.GetAggregateType()
	eventTypes := agg.GetEventTypes()

	var lastEvent *EventMessage

	for i := 0; i < n; i++ {
		agg.IncreaseSequence()
		aggid := agg.GetAggregateID()
		seq := agg.GetSequence()
		eid := eid.CreateEventID(aggid, seq)
		metadata := agg.GetMetadata()
		hashKey := es.createHashKey(aggid, aggType)
		payload, err := json.Marshal(payloads[i])
		if err != nil {
			return err
		}

		events[i] = &EventMessage{
			EventID:       eid,
			EventType:     eventTypes[i],
			AggregateID:   aggid,
			AggregateType: aggType,
			PartitionKey:  agg.GetPartitionKey(),
			HashKey:       hashKey,
			Seq:           seq,
			Payload:       payload,
			Time:          now,
			TimeSeq:       NewSeq(now, seq),
			Metadata:      metadata,
		}

		if len(payloads)-1 == i {
			lastEvent = events[i]
		}
	}

	if lastEvent.Seq == 0 {
		return ErrInvalidVersionNotAllowed
	}

	var snapshot *Snapshot
	if es.snapshotStrategy(agg, events) {
		aggPayload, err := json.Marshal(agg)
		if err != nil {
			return err
		}

		snapshot = &Snapshot{
			AggregateID:   agg.GetAggregateID(),
			AggregateType: aggType,
			HashKey:       lastEvent.HashKey,
			Payload:       aggPayload,
			EventID:       lastEvent.EventID,
			Time:          lastEvent.Time,
			Seq:           lastEvent.Seq,
			TimeSeq:       lastEvent.TimeSeq,
			Metadata:      lastEvent.Metadata,
		}
	}

	if err := es.storage.Save(events, snapshot); err != nil {
		return err
	}

	agg.ClearEvents()

	return nil
}

func (es *eventStore) createHashKey(aggid, aggType string) string {
	return aggid + aggType
}

func WithRetry(numberRetry int, delay time.Duration, fn RetryHandler) error {
	var err error
	currentRetry := 0
	for currentRetry < numberRetry {
		if err = fn(); err != nil {
			if err == ErrVersionInconsistency {
				if delay > 0 {
					time.Sleep(delay)
				}

				currentRetry++
				continue
			}

			return err
		}

		return nil
	}

	return nil
}

func NewSeq(time int64, seq int64) int64 {
	if time < 0 {
		return 0
	}

	if seq < 0 {
		seq = 0
	}

	return (time * 100000) + seq
}
