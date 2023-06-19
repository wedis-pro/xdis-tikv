// Code generated by mockery v2.28.2. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// IKV is an autogenerated mock type for the IKV type
type IKV struct {
	mock.Mock
}

// BatchGet provides a mock function with given fields: ctx, keys
func (_m *IKV) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	ret := _m.Called(ctx, keys)

	var r0 [][]byte
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, [][]byte) ([][]byte, error)); ok {
		return rf(ctx, keys)
	}
	if rf, ok := ret.Get(0).(func(context.Context, [][]byte) [][]byte); ok {
		r0 = rf(ctx, keys)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([][]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, [][]byte) error); ok {
		r1 = rf(ctx, keys)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BatchPut provides a mock function with given fields: ctx, keys, values
func (_m *IKV) BatchPut(ctx context.Context, keys [][]byte, values [][]byte) error {
	ret := _m.Called(ctx, keys, values)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, [][]byte, [][]byte) error); ok {
		r0 = rf(ctx, keys, values)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *IKV) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Get provides a mock function with given fields: ctx, key
func (_m *IKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	ret := _m.Called(ctx, key)

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []byte) ([]byte, error)); ok {
		return rf(ctx, key)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []byte) []byte); ok {
		r0 = rf(ctx, key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []byte) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Incr provides a mock function with given fields: ctx, key, delta
func (_m *IKV) Incr(ctx context.Context, key []byte, delta int64) (int64, error) {
	ret := _m.Called(ctx, key, delta)

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []byte, int64) (int64, error)); ok {
		return rf(ctx, key, delta)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []byte, int64) int64); ok {
		r0 = rf(ctx, key, delta)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, []byte, int64) error); ok {
		r1 = rf(ctx, key, delta)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Put provides a mock function with given fields: ctx, key, value
func (_m *IKV) Put(ctx context.Context, key []byte, value []byte) error {
	ret := _m.Called(ctx, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []byte, []byte) error); ok {
		r0 = rf(ctx, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PutNotExists provides a mock function with given fields: ctx, key, value
func (_m *IKV) PutNotExists(ctx context.Context, key []byte, value []byte) error {
	ret := _m.Called(ctx, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []byte, []byte) error); ok {
		r0 = rf(ctx, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReverseScan provides a mock function with given fields: ctx, startKey, endKey, limit
func (_m *IKV) ReverseScan(ctx context.Context, startKey []byte, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	ret := _m.Called(ctx, startKey, endKey, limit)

	var r0 [][]byte
	var r1 [][]byte
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, []byte, []byte, int) ([][]byte, [][]byte, error)); ok {
		return rf(ctx, startKey, endKey, limit)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []byte, []byte, int) [][]byte); ok {
		r0 = rf(ctx, startKey, endKey, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([][]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []byte, []byte, int) [][]byte); ok {
		r1 = rf(ctx, startKey, endKey, limit)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([][]byte)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, []byte, []byte, int) error); ok {
		r2 = rf(ctx, startKey, endKey, limit)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// Scan provides a mock function with given fields: ctx, startKey, endKey, limit
func (_m *IKV) Scan(ctx context.Context, startKey []byte, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	ret := _m.Called(ctx, startKey, endKey, limit)

	var r0 [][]byte
	var r1 [][]byte
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, []byte, []byte, int) ([][]byte, [][]byte, error)); ok {
		return rf(ctx, startKey, endKey, limit)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []byte, []byte, int) [][]byte); ok {
		r0 = rf(ctx, startKey, endKey, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([][]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []byte, []byte, int) [][]byte); ok {
		r1 = rf(ctx, startKey, endKey, limit)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([][]byte)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, []byte, []byte, int) error); ok {
		r2 = rf(ctx, startKey, endKey, limit)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

type mockConstructorTestingTNewIKV interface {
	mock.TestingT
	Cleanup(func())
}

// NewIKV creates a new instance of IKV. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewIKV(t mockConstructorTestingTNewIKV) *IKV {
	mock := &IKV{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}