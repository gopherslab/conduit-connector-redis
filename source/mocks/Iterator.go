// Code generated by mockery v2.10.0. DO NOT EDIT.

package mocks

import (
	context "context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	mock "github.com/stretchr/testify/mock"
)

// Iterator is an autogenerated mock type for the Iterator type
type Iterator struct {
	mock.Mock
}

// HasNext provides a mock function with given fields:
func (_m *Iterator) HasNext() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Next provides a mock function with given fields: ctx
func (_m *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	ret := _m.Called(ctx)

	var r0 sdk.Record
	if rf, ok := ret.Get(0).(func(context.Context) sdk.Record); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Get(0).(sdk.Record)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Stop provides a mock function with given fields:
func (_m *Iterator) Stop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
