// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import context "context"
import errors "github.com/onedaycat/errors"
import invoke "github.com/onedaycat/zamus/invoke"
import mock "github.com/stretchr/testify/mock"

// Invoker is an autogenerated mock type for the Invoker type
type Invoker struct {
	mock.Mock
}

// BatchInvoke provides a mock function with given fields: ctx, fn, reqs
func (_m *Invoker) BatchInvoke(ctx context.Context, fn string, reqs []*invoke.Request) (invoke.BatchResults, errors.Error) {
	ret := _m.Called(ctx, fn, reqs)

	var r0 invoke.BatchResults
	if rf, ok := ret.Get(0).(func(context.Context, string, []*invoke.Request) invoke.BatchResults); ok {
		r0 = rf(ctx, fn, reqs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(invoke.BatchResults)
		}
	}

	var r1 errors.Error
	if rf, ok := ret.Get(1).(func(context.Context, string, []*invoke.Request) errors.Error); ok {
		r1 = rf(ctx, fn, reqs)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(errors.Error)
		}
	}

	return r0, r1
}

// BatchInvokeAsync provides a mock function with given fields: ctx, fn, reqs
func (_m *Invoker) BatchInvokeAsync(ctx context.Context, fn string, reqs []*invoke.Request) errors.Error {
	ret := _m.Called(ctx, fn, reqs)

	var r0 errors.Error
	if rf, ok := ret.Get(0).(func(context.Context, string, []*invoke.Request) errors.Error); ok {
		r0 = rf(ctx, fn, reqs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errors.Error)
		}
	}

	return r0
}

// Invoke provides a mock function with given fields: ctx, fn, req, result
func (_m *Invoker) Invoke(ctx context.Context, fn string, req *invoke.Request, result interface{}) errors.Error {
	ret := _m.Called(ctx, fn, req, result)

	var r0 errors.Error
	if rf, ok := ret.Get(0).(func(context.Context, string, *invoke.Request, interface{}) errors.Error); ok {
		r0 = rf(ctx, fn, req, result)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errors.Error)
		}
	}

	return r0
}

// InvokeAsync provides a mock function with given fields: ctx, fn, req
func (_m *Invoker) InvokeAsync(ctx context.Context, fn string, req *invoke.Request) errors.Error {
	ret := _m.Called(ctx, fn, req)

	var r0 errors.Error
	if rf, ok := ret.Get(0).(func(context.Context, string, *invoke.Request) errors.Error); ok {
		r0 = rf(ctx, fn, req)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errors.Error)
		}
	}

	return r0
}

// InvokeSaga provides a mock function with given fields: ctx, fn, req
func (_m *Invoker) InvokeSaga(ctx context.Context, fn string, req *invoke.SagaRequest) errors.Error {
	ret := _m.Called(ctx, fn, req)

	var r0 errors.Error
	if rf, ok := ret.Get(0).(func(context.Context, string, *invoke.SagaRequest) errors.Error); ok {
		r0 = rf(ctx, fn, req)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errors.Error)
		}
	}

	return r0
}

// InvokeSagaAsync provides a mock function with given fields: ctx, fn, req
func (_m *Invoker) InvokeSagaAsync(ctx context.Context, fn string, req *invoke.SagaRequest) errors.Error {
	ret := _m.Called(ctx, fn, req)

	var r0 errors.Error
	if rf, ok := ret.Get(0).(func(context.Context, string, *invoke.SagaRequest) errors.Error); ok {
		r0 = rf(ctx, fn, req)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errors.Error)
		}
	}

	return r0
}
