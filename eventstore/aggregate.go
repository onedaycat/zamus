package eventstore

import "github.com/onedaycat/errors"

type AggregateRoot interface {
	Apply(payload *EventMsg) errors.Error
	CurrentVersion() int
	GetAggregateID() string
	SetAggregateID(id string)
	SetSequence(seq int64)
	GetSequence() int64
	SetLastEventTime(t int64)
	GetLastEventTime() int64
	SetLastEventID(id string)
	GetLastEventID() string
	IncreaseSequence()
	GetEvents() []interface{}
	GetEventTypes() []string
	ClearEvents()
	IsNew() bool
	Publish(eventType string, eventData interface{})
}

type AggregateBase struct {
	events         []interface{}
	eventTypes     []string
	seq            int64
	time           int64
	eventid        string
	id             string
	metadata       *Metadata
	currentVersion int
	snpshotVersion int
}

// InitAggregate if id is empty, id will be generated
func InitAggregate(currentVersion int) *AggregateBase {
	return &AggregateBase{
		events:         make([]interface{}, 0, 1),
		eventTypes:     make([]string, 0, 1),
		seq:            0,
		currentVersion: currentVersion,
	}
}

func (a *AggregateBase) GetAggregateID() string {
	return a.id
}

func (a *AggregateBase) SetAggregateID(id string) {
	a.id = id
}

func (a *AggregateBase) Publish(eventType string, eventData interface{}) {
	a.events = append(a.events, eventData)
	a.eventTypes = append(a.eventTypes, eventType)
}

func (a *AggregateBase) GetEvents() []interface{} {
	return a.events
}

func (a *AggregateBase) GetEventTypes() []string {
	return a.eventTypes
}

func (a *AggregateBase) SetSequence(seq int64) {
	a.seq = seq
}

func (a *AggregateBase) ClearEvents() {
	a.events = make([]interface{}, 0, 1)
	a.eventTypes = make([]string, 0, 1)
}

func (a *AggregateBase) IncreaseSequence() {
	a.seq++
}

func (a *AggregateBase) GetSequence() int64 {
	return a.seq
}

func (a *AggregateBase) IsNew() bool {
	if a.id == emptyStr || a.seq == 0 && len(a.events) == 0 {
		return true
	}

	return false
}

func (a *AggregateBase) SetLastEventTime(t int64) {
	a.time = t
}

func (a *AggregateBase) GetLastEventTime() int64 {
	return a.time
}

func (a *AggregateBase) SetLastEventID(id string) {
	a.eventid = id
}

func (a *AggregateBase) GetLastEventID() string {
	return a.eventid
}

func (a *AggregateBase) CurrentVersion() int {
	return a.currentVersion
}

func (a *AggregateBase) SnapshotVersion() int {
	return a.snpshotVersion
}
