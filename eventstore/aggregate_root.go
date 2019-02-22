package eventstore

type AggregateRoot interface {
	Apply(payload *EventMsg) error
	GetAggregateID() string
	SetAggregateID(id string)
	GetAggregateType() string
	SetSequence(seq int64) *AggregateBase
	GetSequence() int64
	IncreaseSequence()
	GetEventPayloads() []interface{}
	GetEventTypes() []string
	ClearEvents()
	IsNew() bool
	Publish(eventType string, eventData interface{})
	GetPartitionKey() string
}

type AggregateBase struct {
	eventPayloads []interface{}
	eventTypes    []string
	seq           int64
	id            string
}

// InitAggregate if id is empty, id will be generated
func InitAggregate() *AggregateBase {
	return &AggregateBase{
		eventPayloads: make([]interface{}, 0, 1),
		eventTypes:    make([]string, 0, 1),
		seq:           0,
	}
}

func (a *AggregateBase) GetAggregateID() string {
	return a.id
}

func (a *AggregateBase) SetAggregateID(id string) {
	a.id = id
}

func (a *AggregateBase) Publish(eventType string, eventData interface{}) {
	a.eventPayloads = append(a.eventPayloads, eventData)
	a.eventTypes = append(a.eventTypes, eventType)
}

func (a *AggregateBase) GetEventPayloads() []interface{} {
	return a.eventPayloads
}

func (a *AggregateBase) GetEventTypes() []string {
	return a.eventTypes
}

func (a *AggregateBase) SetSequence(seq int64) *AggregateBase {
	a.seq = seq

	return a
}

func (a *AggregateBase) ClearEvents() {
	a.eventPayloads = make([]interface{}, 0, 1)
	a.eventTypes = make([]string, 0, 1)
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
