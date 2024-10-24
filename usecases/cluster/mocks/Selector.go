// Code generated by mockery v2.44.1. DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"
	cluster "github.com/weaviate/weaviate/usecases/cluster"
)

// Selector is an autogenerated mock type for the Selector type
type Selector struct {
	mock.Mock
}

// AllHostnames provides a mock function with given fields:
func (_m *Selector) AllHostnames() []string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllHostnames")
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

// AllNames provides a mock function with given fields:
func (_m *Selector) AllNames() []string {
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

// ClusterHealthScore provides a mock function with given fields:
func (_m *Selector) ClusterHealthScore() int {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ClusterHealthScore")
	}

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// Hostnames provides a mock function with given fields:
func (_m *Selector) Hostnames() []string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Hostnames")
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

// LocalName provides a mock function with given fields:
func (_m *Selector) LocalName() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for LocalName")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MaintenanceModeEnabled provides a mock function with given fields:
func (_m *Selector) MaintenanceModeEnabled() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for MaintenanceModeEnabled")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// NodeAddress provides a mock function with given fields: id
func (_m *Selector) NodeAddress(id string) string {
	ret := _m.Called(id)

	if len(ret) == 0 {
		panic("no return value specified for NodeAddress")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func(string) string); ok {
		r0 = rf(id)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// NodeCount provides a mock function with given fields:
func (_m *Selector) NodeCount() int {
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

// NodeHostname provides a mock function with given fields: name
func (_m *Selector) NodeHostname(name string) (string, bool) {
	ret := _m.Called(name)

	if len(ret) == 0 {
		panic("no return value specified for NodeHostname")
	}

	var r0 string
	var r1 bool
	if rf, ok := ret.Get(0).(func(string) (string, bool)); ok {
		return rf(name)
	}
	if rf, ok := ret.Get(0).(func(string) string); ok {
		r0 = rf(name)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(name)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// NodeInfo provides a mock function with given fields: node
func (_m *Selector) NodeInfo(node string) (cluster.NodeInfo, bool) {
	ret := _m.Called(node)

	if len(ret) == 0 {
		panic("no return value specified for NodeInfo")
	}

	var r0 cluster.NodeInfo
	var r1 bool
	if rf, ok := ret.Get(0).(func(string) (cluster.NodeInfo, bool)); ok {
		return rf(node)
	}
	if rf, ok := ret.Get(0).(func(string) cluster.NodeInfo); ok {
		r0 = rf(node)
	} else {
		r0 = ret.Get(0).(cluster.NodeInfo)
	}

	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(node)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// NonStorageNodes provides a mock function with given fields:
func (_m *Selector) NonStorageNodes() []string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for NonStorageNodes")
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

// SchemaSyncIgnored provides a mock function with given fields:
func (_m *Selector) SchemaSyncIgnored() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for SchemaSyncIgnored")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// SkipSchemaRepair provides a mock function with given fields:
func (_m *Selector) SkipSchemaRepair() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for SkipSchemaRepair")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// SortCandidates provides a mock function with given fields: nodes
func (_m *Selector) SortCandidates(nodes []string) []string {
	ret := _m.Called(nodes)

	if len(ret) == 0 {
		panic("no return value specified for SortCandidates")
	}

	var r0 []string
	if rf, ok := ret.Get(0).(func([]string) []string); ok {
		r0 = rf(nodes)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}

// StorageCandidates provides a mock function with given fields:
func (_m *Selector) StorageCandidates() []string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for StorageCandidates")
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

// NewSelector creates a new instance of Selector. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSelector(t interface {
	mock.TestingT
	Cleanup(func())
}) *Selector {
	mock := &Selector{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
