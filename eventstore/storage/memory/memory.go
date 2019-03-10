package memory

import (
	"context"
	"strconv"
	"sync"

	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
)

type MemoryEventStore struct {
	eventstore map[string][]*eventstore.EventMsg
	snapshot   map[string]*eventstore.Snapshot
	locker     sync.Mutex
}

func New() *MemoryEventStore {
	return &MemoryEventStore{
		eventstore: make(map[string][]*eventstore.EventMsg),
		snapshot:   make(map[string]*eventstore.Snapshot),
	}
}

func (d *MemoryEventStore) Truncate() {
	d.locker.Lock()
	defer d.locker.Unlock()

	d.eventstore = make(map[string][]*eventstore.EventMsg)
	d.snapshot = make(map[string]*eventstore.Snapshot)
}

func (d *MemoryEventStore) GetEvents(ctx context.Context, aggID string, seq int64) ([]*eventstore.EventMsg, errors.Error) {
	d.locker.Lock()
	defer d.locker.Unlock()

	store, ok := d.eventstore[aggID]
	if !ok {
		return nil, nil
	}

	msgs := make([]*eventstore.EventMsg, 0, 1000)

	for _, msg := range store {
		if seq < msg.Seq {
			msgs = append(msgs, msg)
		}
	}

	return msgs, nil
}

func (d *MemoryEventStore) GetSnapshot(ctx context.Context, aggID string, version int) (*eventstore.Snapshot, errors.Error) {
	d.locker.Lock()
	defer d.locker.Unlock()

	snap, ok := d.snapshot[aggID+":"+strconv.Itoa(version)]
	if !ok {
		return nil, nil
	}

	return snap, nil
}

func (d *MemoryEventStore) Save(ctx context.Context, msgs []*eventstore.EventMsg, snapshot *eventstore.Snapshot) errors.Error {
	d.locker.Lock()
	defer d.locker.Unlock()

	aggid := msgs[0].AggregateID
	dmsgs, ok := d.eventstore[aggid]
	if !ok {
		d.eventstore[aggid] = make([]*eventstore.EventMsg, 0, 1000)
		dmsgs = d.eventstore[aggid]
	}

	var lastSeq int64
	if len(dmsgs) > 0 {
		lastSeq = dmsgs[len(dmsgs)-1].Seq
	}

	for _, msg := range msgs {
		if lastSeq == msg.Seq {
			return appErr.ErrVersionInconsistency
		}
	}

	d.eventstore[aggid] = append(d.eventstore[aggid], msgs...)
	d.snapshot[snapshot.AggregateID+":"+strconv.Itoa(snapshot.Version)] = snapshot

	return nil
}
