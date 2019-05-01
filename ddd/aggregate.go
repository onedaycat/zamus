package ddd

import (
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
)

const (
	emptyStr = ""
)

type AggregateRoot interface {
	Apply(payload *event.Msg) errors.Error
	GetAggregateID() string
	SetAggregateID(id string)
	SetSequence(seq int64)
	GetSequence() int64
	IncreaseSequence()
	GetEvents() []event.Event
	GetEventTypes() []string
	ClearEvents()
	IsNew() bool
	Publish(eventData event.Event)
}

type AggregateBase struct {
	events     []event.Event
	eventTypes []string
	seq        int64
}

// InitAggregate if id is empty, id will be generated
func InitAggregate() *AggregateBase {
	return &AggregateBase{
		events:     make([]event.Event, 0, 1),
		eventTypes: make([]string, 0, 1),
		seq:        0,
	}
}

func (a *AggregateBase) Publish(eventData event.Event) {
	a.events = append(a.events, eventData)
	a.eventTypes = append(a.eventTypes, event.EventType(eventData))

}

func (a *AggregateBase) GetEvents() []event.Event {
	return a.events
}

func (a *AggregateBase) GetEventTypes() []string {
	return a.eventTypes
}

func (a *AggregateBase) SetSequence(seq int64) {
	a.seq = seq
}

func (a *AggregateBase) ClearEvents() {
	a.events = make([]event.Event, 0, 1)
	a.eventTypes = make([]string, 0, 1)
}

func (a *AggregateBase) IncreaseSequence() {
	a.seq++
}

func (a *AggregateBase) GetSequence() int64 {
	return a.seq
}

func (a *AggregateBase) IsNew() bool {
	if a.seq == 0 && len(a.events) == 0 {
		return true
	}

	return false
}

func (a *AggregateBase) Apply(msg *event.Msg) errors.Error {
	return appErr.ErrNotImplement
}

func (a *AggregateBase) HasEvent(evt event.Event) bool {
	name := event.EventType(evt)
	if name == emptyStr {
		return false
	}

	for _, et := range a.eventTypes {
		if et == name {
			return true
		}
	}

	return false
}
