// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import context "context"
import errors "github.com/onedaycat/errors"
import mock "github.com/stretchr/testify/mock"
import saga "github.com/onedaycat/zamus/saga"

// Storage is an autogenerated mock type for the Storage type
type Storage struct {
	mock.Mock
}

// Get provides a mock function with given fields: ctx, id
func (_m *Storage) Get(ctx context.Context, id string) (*saga.State, errors.Error) {
	ret := _m.Called(ctx, id)

	var r0 *saga.State
	if rf, ok := ret.Get(0).(func(context.Context, string) *saga.State); ok {
		r0 = rf(ctx, id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*saga.State)
		}
	}

	var r1 errors.Error
	if rf, ok := ret.Get(1).(func(context.Context, string) errors.Error); ok {
		r1 = rf(ctx, id)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(errors.Error)
		}
	}

	return r0, r1
}

// Save provides a mock function with given fields: ctx, state
func (_m *Storage) Save(ctx context.Context, state *saga.State) errors.Error {
	ret := _m.Called(ctx, state)

	var r0 errors.Error
	if rf, ok := ret.Get(0).(func(context.Context, *saga.State) errors.Error); ok {
		r0 = rf(ctx, state)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errors.Error)
		}
	}

	return r0
}
