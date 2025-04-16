// Code generated by mockery v2.53.2. DO NOT EDIT.

package authorization

import (
	mock "github.com/stretchr/testify/mock"
	models "github.com/weaviate/weaviate/entities/models"
)

// Authorizer is an autogenerated mock type for the Authorizer type
type Authorizer struct {
	mock.Mock
}

type Authorizer_Expecter struct {
	mock *mock.Mock
}

func (_m *Authorizer) EXPECT() *Authorizer_Expecter {
	return &Authorizer_Expecter{mock: &_m.Mock}
}

// Authorize provides a mock function with given fields: principal, verb, resources
func (_m *Authorizer) Authorize(principal *models.Principal, verb string, resources ...string) error {
	_va := make([]interface{}, len(resources))
	for _i := range resources {
		_va[_i] = resources[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, principal, verb)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Authorize")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*models.Principal, string, ...string) error); ok {
		r0 = rf(principal, verb, resources...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Authorizer_Authorize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Authorize'
type Authorizer_Authorize_Call struct {
	*mock.Call
}

// Authorize is a helper method to define mock.On call
//   - principal *models.Principal
//   - verb string
//   - resources ...string
func (_e *Authorizer_Expecter) Authorize(principal interface{}, verb interface{}, resources ...interface{}) *Authorizer_Authorize_Call {
	return &Authorizer_Authorize_Call{Call: _e.mock.On("Authorize",
		append([]interface{}{principal, verb}, resources...)...)}
}

func (_c *Authorizer_Authorize_Call) Run(run func(principal *models.Principal, verb string, resources ...string)) *Authorizer_Authorize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(*models.Principal), args[1].(string), variadicArgs...)
	})
	return _c
}

func (_c *Authorizer_Authorize_Call) Return(_a0 error) *Authorizer_Authorize_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Authorizer_Authorize_Call) RunAndReturn(run func(*models.Principal, string, ...string) error) *Authorizer_Authorize_Call {
	_c.Call.Return(run)
	return _c
}

// AuthorizeSilent provides a mock function with given fields: principal, verb, resources
func (_m *Authorizer) AuthorizeSilent(principal *models.Principal, verb string, resources ...string) error {
	_va := make([]interface{}, len(resources))
	for _i := range resources {
		_va[_i] = resources[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, principal, verb)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for AuthorizeSilent")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*models.Principal, string, ...string) error); ok {
		r0 = rf(principal, verb, resources...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Authorizer_AuthorizeSilent_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AuthorizeSilent'
type Authorizer_AuthorizeSilent_Call struct {
	*mock.Call
}

// AuthorizeSilent is a helper method to define mock.On call
//   - principal *models.Principal
//   - verb string
//   - resources ...string
func (_e *Authorizer_Expecter) AuthorizeSilent(principal interface{}, verb interface{}, resources ...interface{}) *Authorizer_AuthorizeSilent_Call {
	return &Authorizer_AuthorizeSilent_Call{Call: _e.mock.On("AuthorizeSilent",
		append([]interface{}{principal, verb}, resources...)...)}
}

func (_c *Authorizer_AuthorizeSilent_Call) Run(run func(principal *models.Principal, verb string, resources ...string)) *Authorizer_AuthorizeSilent_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(*models.Principal), args[1].(string), variadicArgs...)
	})
	return _c
}

func (_c *Authorizer_AuthorizeSilent_Call) Return(_a0 error) *Authorizer_AuthorizeSilent_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Authorizer_AuthorizeSilent_Call) RunAndReturn(run func(*models.Principal, string, ...string) error) *Authorizer_AuthorizeSilent_Call {
	_c.Call.Return(run)
	return _c
}

// FilterAuthorizedResources provides a mock function with given fields: principal, verb, resources
func (_m *Authorizer) FilterAuthorizedResources(principal *models.Principal, verb string, resources ...string) ([]string, error) {
	_va := make([]interface{}, len(resources))
	for _i := range resources {
		_va[_i] = resources[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, principal, verb)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for FilterAuthorizedResources")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(*models.Principal, string, ...string) ([]string, error)); ok {
		return rf(principal, verb, resources...)
	}
	if rf, ok := ret.Get(0).(func(*models.Principal, string, ...string) []string); ok {
		r0 = rf(principal, verb, resources...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(*models.Principal, string, ...string) error); ok {
		r1 = rf(principal, verb, resources...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Authorizer_FilterAuthorizedResources_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'FilterAuthorizedResources'
type Authorizer_FilterAuthorizedResources_Call struct {
	*mock.Call
}

// FilterAuthorizedResources is a helper method to define mock.On call
//   - principal *models.Principal
//   - verb string
//   - resources ...string
func (_e *Authorizer_Expecter) FilterAuthorizedResources(principal interface{}, verb interface{}, resources ...interface{}) *Authorizer_FilterAuthorizedResources_Call {
	return &Authorizer_FilterAuthorizedResources_Call{Call: _e.mock.On("FilterAuthorizedResources",
		append([]interface{}{principal, verb}, resources...)...)}
}

func (_c *Authorizer_FilterAuthorizedResources_Call) Run(run func(principal *models.Principal, verb string, resources ...string)) *Authorizer_FilterAuthorizedResources_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(*models.Principal), args[1].(string), variadicArgs...)
	})
	return _c
}

func (_c *Authorizer_FilterAuthorizedResources_Call) Return(_a0 []string, _a1 error) *Authorizer_FilterAuthorizedResources_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *Authorizer_FilterAuthorizedResources_Call) RunAndReturn(run func(*models.Principal, string, ...string) ([]string, error)) *Authorizer_FilterAuthorizedResources_Call {
	_c.Call.Return(run)
	return _c
}

// NewAuthorizer creates a new instance of Authorizer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewAuthorizer(t interface {
	mock.TestingT
	Cleanup(func())
}) *Authorizer {
	mock := &Authorizer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
