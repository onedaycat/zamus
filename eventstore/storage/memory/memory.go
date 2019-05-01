package memory

import (
	"context"
	"sync"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
)

type EventStoreStroage struct {
	eventstore []*event.Msg
	locker     sync.Mutex
}

func New() *EventStoreStroage {
	return &EventStoreStroage{
		eventstore: make([]*event.Msg, 0),
	}
}

func (d *EventStoreStroage) Truncate() {
	d.locker.Lock()
	defer d.locker.Unlock()

	d.eventstore = make([]*event.Msg, 0)
}

func (d *EventStoreStroage) Save(ctx context.Context, msgs event.Msgs) errors.Error {
	d.locker.Lock()
	defer d.locker.Unlock()

	if err := d.saveEvents(ctx, msgs); err != nil {
		return err
	}

	return nil
}

func (d *EventStoreStroage) saveEvents(ctx context.Context, msgs event.Msgs) errors.Error {
	for _, msg := range msgs {
		d.eventstore = append(d.eventstore, msg)
	}

	return nil
}
