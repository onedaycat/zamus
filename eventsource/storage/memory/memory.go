package memory

import (
	"context"
	"strconv"
	"sync"

	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/eventsource"
)

type EventSourceStroage struct {
	eventsource map[string]event.Msgs
	snapshot    map[string]*eventsource.Snapshot
	locker      sync.Mutex
}

func New() *EventSourceStroage {
	return &EventSourceStroage{
		eventsource: make(map[string]event.Msgs),
		snapshot:    make(map[string]*eventsource.Snapshot),
	}
}

func (d *EventSourceStroage) Truncate() {
	d.locker.Lock()
	defer d.locker.Unlock()

	d.eventsource = make(map[string]event.Msgs)
	d.snapshot = make(map[string]*eventsource.Snapshot)
}

func (d *EventSourceStroage) GetEvents(ctx context.Context, aggID string, seq int64) (event.Msgs, errors.Error) {
	d.locker.Lock()
	defer d.locker.Unlock()

	store, ok := d.eventsource[aggID]
	if !ok {
		return nil, nil
	}

	msgs := make(event.Msgs, 0, 100)

	for _, msg := range store {
		if seq < msg.Seq {
			msgs = append(msgs, msg)
		}
	}

	if len(msgs) == 0 {
		return nil, nil
	}

	return msgs, nil
}

func (d *EventSourceStroage) GetSnapshot(ctx context.Context, aggID string, version int) (*eventsource.Snapshot, errors.Error) {
	d.locker.Lock()
	defer d.locker.Unlock()

	if version == 0 {
		return nil, nil
	}

	snap, ok := d.snapshot[aggID+":"+strconv.Itoa(version)]
	if !ok {
		return nil, nil
	}

	return snap, nil
}

func (d *EventSourceStroage) Save(ctx context.Context, msgs event.Msgs, snapshot *eventsource.Snapshot) errors.Error {
	d.locker.Lock()
	defer d.locker.Unlock()

	if err := d.saveEvents(ctx, msgs); err != nil {
		return err
	}

	return d.saveSnapshot(ctx, snapshot)
}

func (d *EventSourceStroage) saveEvents(ctx context.Context, msgs event.Msgs) errors.Error {
	aggid := msgs[0].AggID
	dmsgs, ok := d.eventsource[aggid]
	if !ok {
		d.eventsource[aggid] = make(event.Msgs, 0, 100)
		dmsgs = d.eventsource[aggid]
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

	d.eventsource[aggid] = append(d.eventsource[aggid], msgs...)

	return nil
}

func (d *EventSourceStroage) saveSnapshot(ctx context.Context, snapshot *eventsource.Snapshot) errors.Error {
	if snapshot == nil {
		return nil
	}

	d.snapshot[snapshot.AggID+":"+strconv.Itoa(snapshot.Version)] = snapshot

	return nil
}
