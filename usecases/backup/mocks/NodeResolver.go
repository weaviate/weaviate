//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// NodeResolver is an autogenerated mock type for the NodeResolver type
type NodeResolver struct {
	mock.Mock
}

// AllNames provides a mock function with given fields:
func (_m *NodeResolver) AllNames() []string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllNames")
	}

	var r0 []string
	if rf, ok := ret.Get(0).(func() []string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}

// LeaderID provides a mock function with given fields:
func (_m *NodeResolver) LeaderID() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for LeaderID")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// NodeCount provides a mock function with given fields:
func (_m *NodeResolver) NodeCount() int {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for NodeCount")
	}

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// NodeHostname provides a mock function with given fields: nodeName
func (_m *NodeResolver) NodeHostname(nodeName string) (string, bool) {
	ret := _m.Called(nodeName)

	if len(ret) == 0 {
		panic("no return value specified for NodeHostname")
	}

	var r0 string
	var r1 bool
	if rf, ok := ret.Get(0).(func(string) (string, bool)); ok {
		return rf(nodeName)
	}
	if rf, ok := ret.Get(0).(func(string) string); ok {
		r0 = rf(nodeName)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(nodeName)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// NewNodeResolver creates a new instance of NodeResolver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewNodeResolver(t interface {
	mock.TestingT
	Cleanup(func())
}) *NodeResolver {
	mock := &NodeResolver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
