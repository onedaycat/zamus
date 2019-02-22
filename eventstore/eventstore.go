package eventstore

import (
	"encoding/json"
	"time"

	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/common/eid"
)

//go:generate mockery -name=EventStore
//go:generate protoc --gofast_out=. event.proto

// RetryHandler if return bool is true is allow retry,
// if return bool is false no retry
type RetryHandler func() error

const emptyStr = ""

type SaveOption func(opt *saveOption)

type saveOption struct {
	userID string
}

func WithUser(id string) SaveOption {
	return func(opt *saveOption) {
		opt.userID = id
	}
}

type EventStore interface {
	SetEventLimit(limit int64)
	// SetSnapshotStrategy(strategies SnapshotStategyHandler)
	GetEvents(aggID string, seq int64, agg AggregateRoot) ([]*EventMsg, error)
	// GetEventsByEventType(eventType EventType, seq int64) ([]*EventMsg, error)
	// GetEventsByAggregateType(aggType string, seq int64) ([]*EventMsg, error)
	GetAggregate(aggID string, agg AggregateRoot) error
	// GetAggregateByEvents(aggID string, agg AggregateRoot) error
	// GetSnapshot(aggID string, agg AggregateRoot) error
	Save(agg AggregateRoot, options ...SaveOption) error
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

func (es *eventStore) GetAggregateByEvents(id string, agg AggregateRoot) error {
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

func (es *eventStore) GetEvents(id string, seq int64, agg AggregateRoot) ([]*EventMsg, error) {
	hashKey := es.createHashKey(id, agg.GetAggregateType())
	return es.storage.GetEvents(id, hashKey, seq, es.limit)
}

func (es *eventStore) GetEventsByEventType(eventType string, seq int64) ([]*EventMsg, error) {
	return es.storage.GetEventsByEventType(eventType, seq, es.limit)
}

func (es *eventStore) GetEventsByAggregateType(aggType string, seq int64) ([]*EventMsg, error) {
	return es.storage.GetEventsByAggregateType(aggType, seq, es.limit)
}

func (es *eventStore) GetAggregate(id string, agg AggregateRoot) error {
	hashKey := es.createHashKey(id, agg.GetAggregateType())
	snapshot, err := es.storage.GetAggregate(id, hashKey)
	if err != nil {
		return err
	}

	agg.SetAggregateID(snapshot.AggregateID)
	agg.SetSequence(snapshot.Seq)

	if err = json.Unmarshal(snapshot.Data, agg); err != nil {
		return err
	}

	return nil
}

func (es *eventStore) GetSnapshot(id string, agg AggregateRoot) error {
	hashKey := es.createHashKey(id, agg.GetAggregateType())
	snapshot, err := es.storage.GetSnapshot(id, hashKey)
	if err != nil {
		return err
	}

	agg.SetAggregateID(snapshot.AggregateID)
	agg.SetSequence(snapshot.Seq)

	if err = json.Unmarshal(snapshot.Data, agg); err != nil {
		return err
	}

	return nil
}

func (es *eventStore) Save(agg AggregateRoot, options ...SaveOption) error {
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

	opts := &saveOption{}
	for _, option := range options {
		option(opts)
	}

	events := make([]*EventMsg, n)
	now := clock.Now().Unix()
	aggType := agg.GetAggregateType()
	eventTypes := agg.GetEventTypes()

	var lastEvent *EventMsg

	for i := 0; i < n; i++ {
		agg.IncreaseSequence()
		aggid := agg.GetAggregateID()
		seq := agg.GetSequence()
		eid := eid.CreateEventID(aggid, seq)
		userID := opts.userID
		hashKey := es.createHashKey(aggid, aggType)
		data, err := json.Marshal(payloads[i])
		if err != nil {
			return err
		}

		events[i] = &EventMsg{
			EventID:       eid,
			EventType:     eventTypes[i],
			AggregateID:   aggid,
			AggregateType: aggType,
			PartitionKey:  agg.GetPartitionKey(),
			HashKey:       hashKey,
			Seq:           seq,
			Data:          data,
			Time:          now,
			TimeSeq:       NewSeq(now, seq),
			UserID:        userID,
		}

		if len(payloads)-1 == i {
			lastEvent = events[i]
		}
	}

	if lastEvent.Seq == 0 {
		return ErrInvalidVersionNotAllowed
	}

	var aggmsg *AggregateMsg
	aggData, err := json.Marshal(agg)
	if err != nil {
		return err
	}

	aggmsg = &AggregateMsg{
		AggregateID: agg.GetAggregateID(),
		HashKey:     lastEvent.HashKey,
		Data:        aggData,
		EventID:     lastEvent.EventID,
		Time:        lastEvent.Time,
		Seq:         lastEvent.Seq,
		TimeSeq:     lastEvent.TimeSeq,
	}

	if err := es.storage.Save(events, aggmsg); err != nil {
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
