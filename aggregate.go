package gocqrs

type AggregateType = string

type AggregateRoot interface {
	Apply(payload *EventMessage) error
	GetAggregateID() string
	SetAggregateID(id string)
	GetAggregateType() string
	SetSequence(seq int64) *AggregateBase
	GetSequence() int64
	SetMetadata(metadata *Metadata)
	GetMetadata() *Metadata
	IncreaseSequence()
	GetEventPayloads() []interface{}
	GetEventTypes() []EventType
	ClearEvents()
	IsNew() bool
	Publish(eventType EventType, event interface{})
	GetPartitionKey() string
}

type AggregateBase struct {
	eventPayloads []interface{}
	eventTypes    []EventType
	seq           int64
	metadata      *Metadata
}

// InitAggregate if id is empty, id will be generated
func InitAggregate() *AggregateBase {
	return &AggregateBase{
		eventPayloads: make([]interface{}, 0, 1),
		eventTypes:    make([]EventType, 0, 1),
		seq:           0,
	}
}

func (a *AggregateBase) Publish(eventType EventType, event interface{}) {
	a.eventPayloads = append(a.eventPayloads, event)
	a.eventTypes = append(a.eventTypes, eventType)
}

func (a *AggregateBase) GetEventPayloads() []interface{} {
	return a.eventPayloads
}

func (a *AggregateBase) GetEventTypes() []EventType {
	return a.eventTypes
}

func (a *AggregateBase) SetSequence(seq int64) *AggregateBase {
	a.seq = seq

	return a
}

func (a *AggregateBase) ClearEvents() {
	a.eventPayloads = make([]interface{}, 0, 1)
	a.eventTypes = make([]EventType, 0, 1)
}

func (a *AggregateBase) IncreaseSequence() {
	a.seq++
}

func (a *AggregateBase) GetSequence() int64 {
	return a.seq
}

func (a *AggregateBase) IsNew() bool {
	return a.seq == 0
}

func (a *AggregateBase) SetMetadata(metadata *Metadata) {
	a.metadata = metadata
}

func (a *AggregateBase) GetMetadata() *Metadata {
	return a.metadata
}
