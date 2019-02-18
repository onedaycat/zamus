// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import eventstore "github.com/onedaycat/zamus/eventstore"
import mock "github.com/stretchr/testify/mock"

// Storage is an autogenerated mock type for the Storage type
type Storage struct {
	mock.Mock
}

// GetAggregate provides a mock function with given fields: aggID, hashKey
func (_m *Storage) GetAggregate(aggID string, hashKey string) (*eventstore.AggregateMsg, error) {
	ret := _m.Called(aggID, hashKey)

	var r0 *eventstore.AggregateMsg
	if rf, ok := ret.Get(0).(func(string, string) *eventstore.AggregateMsg); ok {
		r0 = rf(aggID, hashKey)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*eventstore.AggregateMsg)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(aggID, hashKey)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetEvents provides a mock function with given fields: aggID, hashKey, seq, limit
func (_m *Storage) GetEvents(aggID string, hashKey string, seq int64, limit int64) ([]*eventstore.EventMsg, error) {
	ret := _m.Called(aggID, hashKey, seq, limit)

	var r0 []*eventstore.EventMsg
	if rf, ok := ret.Get(0).(func(string, string, int64, int64) []*eventstore.EventMsg); ok {
		r0 = rf(aggID, hashKey, seq, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*eventstore.EventMsg)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string, int64, int64) error); ok {
		r1 = rf(aggID, hashKey, seq, limit)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetEventsByAggregateType provides a mock function with given fields: aggType, seq, limit
func (_m *Storage) GetEventsByAggregateType(aggType string, seq int64, limit int64) ([]*eventstore.EventMsg, error) {
	ret := _m.Called(aggType, seq, limit)

	var r0 []*eventstore.EventMsg
	if rf, ok := ret.Get(0).(func(string, int64, int64) []*eventstore.EventMsg); ok {
		r0 = rf(aggType, seq, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*eventstore.EventMsg)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int64, int64) error); ok {
		r1 = rf(aggType, seq, limit)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetEventsByEventType provides a mock function with given fields: eventType, seq, limit
func (_m *Storage) GetEventsByEventType(eventType string, seq int64, limit int64) ([]*eventstore.EventMsg, error) {
	ret := _m.Called(eventType, seq, limit)

	var r0 []*eventstore.EventMsg
	if rf, ok := ret.Get(0).(func(string, int64, int64) []*eventstore.EventMsg); ok {
		r0 = rf(eventType, seq, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*eventstore.EventMsg)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int64, int64) error); ok {
		r1 = rf(eventType, seq, limit)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetSnapshot provides a mock function with given fields: aggID, hashKey
func (_m *Storage) GetSnapshot(aggID string, hashKey string) (*eventstore.SnapshotMsg, error) {
	ret := _m.Called(aggID, hashKey)

	var r0 *eventstore.SnapshotMsg
	if rf, ok := ret.Get(0).(func(string, string) *eventstore.SnapshotMsg); ok {
		r0 = rf(aggID, hashKey)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*eventstore.SnapshotMsg)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(aggID, hashKey)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Save provides a mock function with given fields: events, agg
func (_m *Storage) Save(events []*eventstore.EventMsg, agg *eventstore.AggregateMsg) error {
	ret := _m.Called(events, agg)

	var r0 error
	if rf, ok := ret.Get(0).(func([]*eventstore.EventMsg, *eventstore.AggregateMsg) error); ok {
		r0 = rf(events, agg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
