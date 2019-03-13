package replay

import "time"

type Reply struct {
	storage    Storage
	fromTime   int64
	toTime     int64
	eventTypes []string
	aggID      string
}

func New(storage Storage) *Reply {
	return &Reply{
		storage: storage,
	}
}

func (r *Reply) Start(from time.Time) *Reply {
	r.fromTime = from.Unix()

	return r
}

func (r *Reply) To(to time.Time) *Reply {
	r.toTime = to.Unix()

	return r
}

func (r *Reply) Range(from, to time.Time) *Reply {
	r.fromTime = from.Unix()
	r.toTime = to.Unix()

	return r
}

func (r *Reply) EveentTypes(eventTypes ...string) *Reply {
	r.eventTypes = eventTypes

	return r
}

func (r *Reply) AggregateID(aggID string) *Reply {
	r.aggID = aggID

	return r
}

func (r *Reply) Run() {
	r.storage.Query(r.fromTime, r.toTime, r.eventTypes, r.aggID)
}
