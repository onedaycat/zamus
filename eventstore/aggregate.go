package eventstore

import (
    "github.com/gogo/protobuf/proto"
    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
)

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
    GetEvents() []proto.Message
    GetEventTypes() []string
    ClearEvents()
    IsNew() bool
    Publish(eventData proto.Message)
}

type AggregateBase struct {
    events         []proto.Message
    eventTypes     []string
    seq            int64
    time           int64
    eventid        string
    id             string
    currentVersion int
    snpshotVersion int
}

// InitAggregate if id is empty, id will be generated
func InitAggregate(currentVersion int) *AggregateBase {
    return &AggregateBase{
        events:         make([]proto.Message, 0, 1),
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

func (a *AggregateBase) Publish(eventData proto.Message) {
    a.events = append(a.events, eventData)
    a.eventTypes = append(a.eventTypes, proto.MessageName(eventData))
}

func (a *AggregateBase) GetEvents() []proto.Message {
    return a.events
}

func (a *AggregateBase) GetEventTypes() []string {
    return a.eventTypes
}

func (a *AggregateBase) SetSequence(seq int64) {
    a.seq = seq
}

func (a *AggregateBase) ClearEvents() {
    a.events = make([]proto.Message, 0, 1)
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

func (a *AggregateBase) Apply(msg *EventMsg) errors.Error {
    return appErr.ErrNotImplement
}

func (a *AggregateBase) HasEvent(eventType string) bool {
    for _, et := range a.eventTypes {
        if et == eventType {
            return true
        }
    }

    return false
}
