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

// Code generated by mockery v2.52.3. DO NOT EDIT.

package mocks

import (
	io "io"

	mock "github.com/stretchr/testify/mock"

	authorization "github.com/weaviate/weaviate/usecases/auth/authorization"
)

// Controller is an autogenerated mock type for the Controller type
type Controller struct {
	mock.Mock
}

// AddRolesForUser provides a mock function with given fields: user, roles
func (_m *Controller) AddRolesForUser(user string, roles []string) error {
	ret := _m.Called(user, roles)

	if len(ret) == 0 {
		panic("no return value specified for AddRolesForUser")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []string) error); ok {
		r0 = rf(user, roles)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateRolesPermissions provides a mock function with given fields: roles
func (_m *Controller) CreateRolesPermissions(roles map[string][]authorization.Policy) error {
	ret := _m.Called(roles)

	if len(ret) == 0 {
		panic("no return value specified for CreateRolesPermissions")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(map[string][]authorization.Policy) error); ok {
		r0 = rf(roles)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteRoles provides a mock function with given fields: roles
func (_m *Controller) DeleteRoles(roles ...string) error {
	_va := make([]interface{}, len(roles))
	for _i := range roles {
		_va[_i] = roles[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for DeleteRoles")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(...string) error); ok {
		r0 = rf(roles...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetRoles provides a mock function with given fields: names
func (_m *Controller) GetRoles(names ...string) (map[string][]authorization.Policy, error) {
	_va := make([]interface{}, len(names))
	for _i := range names {
		_va[_i] = names[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetRoles")
	}

	var r0 map[string][]authorization.Policy
	var r1 error
	if rf, ok := ret.Get(0).(func(...string) (map[string][]authorization.Policy, error)); ok {
		return rf(names...)
	}
	if rf, ok := ret.Get(0).(func(...string) map[string][]authorization.Policy); ok {
		r0 = rf(names...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]authorization.Policy)
		}
	}

	if rf, ok := ret.Get(1).(func(...string) error); ok {
		r1 = rf(names...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRolesForUser provides a mock function with given fields: user
func (_m *Controller) GetRolesForUser(user string) (map[string][]authorization.Policy, error) {
	ret := _m.Called(user)

	if len(ret) == 0 {
		panic("no return value specified for GetRolesForUser")
	}

	var r0 map[string][]authorization.Policy
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (map[string][]authorization.Policy, error)); ok {
		return rf(user)
	}
	if rf, ok := ret.Get(0).(func(string) map[string][]authorization.Policy); ok {
		r0 = rf(user)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]authorization.Policy)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(user)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetUsersForRole provides a mock function with given fields: role
func (_m *Controller) GetUsersForRole(role string) ([]string, error) {
	ret := _m.Called(role)

	if len(ret) == 0 {
		panic("no return value specified for GetUsersForRole")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(string) ([]string, error)); ok {
		return rf(role)
	}
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(role)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(role)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// HasPermission provides a mock function with given fields: role, permission
func (_m *Controller) HasPermission(role string, permission *authorization.Policy) (bool, error) {
	ret := _m.Called(role, permission)

	if len(ret) == 0 {
		panic("no return value specified for HasPermission")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(string, *authorization.Policy) (bool, error)); ok {
		return rf(role, permission)
	}
	if rf, ok := ret.Get(0).(func(string, *authorization.Policy) bool); ok {
		r0 = rf(role, permission)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(string, *authorization.Policy) error); ok {
		r1 = rf(role, permission)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemovePermissions provides a mock function with given fields: role, permissions
func (_m *Controller) RemovePermissions(role string, permissions []*authorization.Policy) error {
	ret := _m.Called(role, permissions)

	if len(ret) == 0 {
		panic("no return value specified for RemovePermissions")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []*authorization.Policy) error); ok {
		r0 = rf(role, permissions)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Restore provides a mock function with given fields: r
func (_m *Controller) Restore(r io.Reader) error {
	ret := _m.Called(r)

	if len(ret) == 0 {
		panic("no return value specified for Restore")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(io.Reader) error); ok {
		r0 = rf(r)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RevokeRolesForUser provides a mock function with given fields: user, roles
func (_m *Controller) RevokeRolesForUser(user string, roles ...string) error {
	_va := make([]interface{}, len(roles))
	for _i := range roles {
		_va[_i] = roles[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, user)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for RevokeRolesForUser")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, ...string) error); ok {
		r0 = rf(user, roles...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Snapshot provides a mock function with no fields
func (_m *Controller) Snapshot() ([]byte, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Snapshot")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]byte, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateRolesPermissions provides a mock function with given fields: roles
func (_m *Controller) UpdateRolesPermissions(roles map[string][]authorization.Policy) error {
	ret := _m.Called(roles)

	if len(ret) == 0 {
		panic("no return value specified for UpdateRolesPermissions")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(map[string][]authorization.Policy) error); ok {
		r0 = rf(roles)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewController creates a new instance of Controller. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewController(t interface {
	mock.TestingT
	Cleanup(func())
}) *Controller {
	mock := &Controller{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
