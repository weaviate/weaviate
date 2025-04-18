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

// Code generated by mockery v2.53.2. DO NOT EDIT.

package modulecapabilities

import (
	context "context"

	backup "github.com/weaviate/weaviate/entities/backup"

	io "io"

	mock "github.com/stretchr/testify/mock"
)

// MockBackupBackend is an autogenerated mock type for the BackupBackend type
type MockBackupBackend struct {
	mock.Mock
}

type MockBackupBackend_Expecter struct {
	mock *mock.Mock
}

func (_m *MockBackupBackend) EXPECT() *MockBackupBackend_Expecter {
	return &MockBackupBackend_Expecter{mock: &_m.Mock}
}

// AllBackups provides a mock function with given fields: ctx
func (_m *MockBackupBackend) AllBackups(ctx context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for AllBackups")
	}

	var r0 []*backup.DistributedBackupDescriptor
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]*backup.DistributedBackupDescriptor, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []*backup.DistributedBackupDescriptor); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*backup.DistributedBackupDescriptor)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBackupBackend_AllBackups_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllBackups'
type MockBackupBackend_AllBackups_Call struct {
	*mock.Call
}

// AllBackups is a helper method to define mock.On call
//   - ctx context.Context
func (_e *MockBackupBackend_Expecter) AllBackups(ctx interface{}) *MockBackupBackend_AllBackups_Call {
	return &MockBackupBackend_AllBackups_Call{Call: _e.mock.On("AllBackups", ctx)}
}

func (_c *MockBackupBackend_AllBackups_Call) Run(run func(ctx context.Context)) *MockBackupBackend_AllBackups_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *MockBackupBackend_AllBackups_Call) Return(_a0 []*backup.DistributedBackupDescriptor, _a1 error) *MockBackupBackend_AllBackups_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBackupBackend_AllBackups_Call) RunAndReturn(run func(context.Context) ([]*backup.DistributedBackupDescriptor, error)) *MockBackupBackend_AllBackups_Call {
	_c.Call.Return(run)
	return _c
}

// GetObject provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath
func (_m *MockBackupBackend) GetObject(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string) ([]byte, error) {
	ret := _m.Called(ctx, backupID, key, overrideBucket, overridePath)

	if len(ret) == 0 {
		panic("no return value specified for GetObject")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string) ([]byte, error)); ok {
		return rf(ctx, backupID, key, overrideBucket, overridePath)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string) []byte); ok {
		r0 = rf(ctx, backupID, key, overrideBucket, overridePath)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, string, string) error); ok {
		r1 = rf(ctx, backupID, key, overrideBucket, overridePath)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBackupBackend_GetObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetObject'
type MockBackupBackend_GetObject_Call struct {
	*mock.Call
}

// GetObject is a helper method to define mock.On call
//   - ctx context.Context
//   - backupID string
//   - key string
//   - overrideBucket string
//   - overridePath string
func (_e *MockBackupBackend_Expecter) GetObject(ctx interface{}, backupID interface{}, key interface{}, overrideBucket interface{}, overridePath interface{}) *MockBackupBackend_GetObject_Call {
	return &MockBackupBackend_GetObject_Call{Call: _e.mock.On("GetObject", ctx, backupID, key, overrideBucket, overridePath)}
}

func (_c *MockBackupBackend_GetObject_Call) Run(run func(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string)) *MockBackupBackend_GetObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string))
	})
	return _c
}

func (_c *MockBackupBackend_GetObject_Call) Return(_a0 []byte, _a1 error) *MockBackupBackend_GetObject_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBackupBackend_GetObject_Call) RunAndReturn(run func(context.Context, string, string, string, string) ([]byte, error)) *MockBackupBackend_GetObject_Call {
	_c.Call.Return(run)
	return _c
}

// HomeDir provides a mock function with given fields: backupID, overrideBucket, overridePath
func (_m *MockBackupBackend) HomeDir(backupID string, overrideBucket string, overridePath string) string {
	ret := _m.Called(backupID, overrideBucket, overridePath)

	if len(ret) == 0 {
		panic("no return value specified for HomeDir")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func(string, string, string) string); ok {
		r0 = rf(backupID, overrideBucket, overridePath)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockBackupBackend_HomeDir_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HomeDir'
type MockBackupBackend_HomeDir_Call struct {
	*mock.Call
}

// HomeDir is a helper method to define mock.On call
//   - backupID string
//   - overrideBucket string
//   - overridePath string
func (_e *MockBackupBackend_Expecter) HomeDir(backupID interface{}, overrideBucket interface{}, overridePath interface{}) *MockBackupBackend_HomeDir_Call {
	return &MockBackupBackend_HomeDir_Call{Call: _e.mock.On("HomeDir", backupID, overrideBucket, overridePath)}
}

func (_c *MockBackupBackend_HomeDir_Call) Run(run func(backupID string, overrideBucket string, overridePath string)) *MockBackupBackend_HomeDir_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *MockBackupBackend_HomeDir_Call) Return(_a0 string) *MockBackupBackend_HomeDir_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_HomeDir_Call) RunAndReturn(run func(string, string, string) string) *MockBackupBackend_HomeDir_Call {
	_c.Call.Return(run)
	return _c
}

// Initialize provides a mock function with given fields: ctx, backupID, overrideBucket, overridePath
func (_m *MockBackupBackend) Initialize(ctx context.Context, backupID string, overrideBucket string, overridePath string) error {
	ret := _m.Called(ctx, backupID, overrideBucket, overridePath)

	if len(ret) == 0 {
		panic("no return value specified for Initialize")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string) error); ok {
		r0 = rf(ctx, backupID, overrideBucket, overridePath)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockBackupBackend_Initialize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Initialize'
type MockBackupBackend_Initialize_Call struct {
	*mock.Call
}

// Initialize is a helper method to define mock.On call
//   - ctx context.Context
//   - backupID string
//   - overrideBucket string
//   - overridePath string
func (_e *MockBackupBackend_Expecter) Initialize(ctx interface{}, backupID interface{}, overrideBucket interface{}, overridePath interface{}) *MockBackupBackend_Initialize_Call {
	return &MockBackupBackend_Initialize_Call{Call: _e.mock.On("Initialize", ctx, backupID, overrideBucket, overridePath)}
}

func (_c *MockBackupBackend_Initialize_Call) Run(run func(ctx context.Context, backupID string, overrideBucket string, overridePath string)) *MockBackupBackend_Initialize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string))
	})
	return _c
}

func (_c *MockBackupBackend_Initialize_Call) Return(_a0 error) *MockBackupBackend_Initialize_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_Initialize_Call) RunAndReturn(run func(context.Context, string, string, string) error) *MockBackupBackend_Initialize_Call {
	_c.Call.Return(run)
	return _c
}

// IsExternal provides a mock function with no fields
func (_m *MockBackupBackend) IsExternal() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for IsExternal")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// MockBackupBackend_IsExternal_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsExternal'
type MockBackupBackend_IsExternal_Call struct {
	*mock.Call
}

// IsExternal is a helper method to define mock.On call
func (_e *MockBackupBackend_Expecter) IsExternal() *MockBackupBackend_IsExternal_Call {
	return &MockBackupBackend_IsExternal_Call{Call: _e.mock.On("IsExternal")}
}

func (_c *MockBackupBackend_IsExternal_Call) Run(run func()) *MockBackupBackend_IsExternal_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockBackupBackend_IsExternal_Call) Return(_a0 bool) *MockBackupBackend_IsExternal_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_IsExternal_Call) RunAndReturn(run func() bool) *MockBackupBackend_IsExternal_Call {
	_c.Call.Return(run)
	return _c
}

// Name provides a mock function with no fields
func (_m *MockBackupBackend) Name() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Name")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockBackupBackend_Name_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Name'
type MockBackupBackend_Name_Call struct {
	*mock.Call
}

// Name is a helper method to define mock.On call
func (_e *MockBackupBackend_Expecter) Name() *MockBackupBackend_Name_Call {
	return &MockBackupBackend_Name_Call{Call: _e.mock.On("Name")}
}

func (_c *MockBackupBackend_Name_Call) Run(run func()) *MockBackupBackend_Name_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockBackupBackend_Name_Call) Return(_a0 string) *MockBackupBackend_Name_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_Name_Call) RunAndReturn(run func() string) *MockBackupBackend_Name_Call {
	_c.Call.Return(run)
	return _c
}

// PutObject provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath, byes
func (_m *MockBackupBackend) PutObject(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, byes []byte) error {
	ret := _m.Called(ctx, backupID, key, overrideBucket, overridePath, byes)

	if len(ret) == 0 {
		panic("no return value specified for PutObject")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, []byte) error); ok {
		r0 = rf(ctx, backupID, key, overrideBucket, overridePath, byes)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockBackupBackend_PutObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PutObject'
type MockBackupBackend_PutObject_Call struct {
	*mock.Call
}

// PutObject is a helper method to define mock.On call
//   - ctx context.Context
//   - backupID string
//   - key string
//   - overrideBucket string
//   - overridePath string
//   - byes []byte
func (_e *MockBackupBackend_Expecter) PutObject(ctx interface{}, backupID interface{}, key interface{}, overrideBucket interface{}, overridePath interface{}, byes interface{}) *MockBackupBackend_PutObject_Call {
	return &MockBackupBackend_PutObject_Call{Call: _e.mock.On("PutObject", ctx, backupID, key, overrideBucket, overridePath, byes)}
}

func (_c *MockBackupBackend_PutObject_Call) Run(run func(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, byes []byte)) *MockBackupBackend_PutObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].([]byte))
	})
	return _c
}

func (_c *MockBackupBackend_PutObject_Call) Return(_a0 error) *MockBackupBackend_PutObject_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_PutObject_Call) RunAndReturn(run func(context.Context, string, string, string, string, []byte) error) *MockBackupBackend_PutObject_Call {
	_c.Call.Return(run)
	return _c
}

// Read provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath, w
func (_m *MockBackupBackend) Read(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, w io.WriteCloser) (int64, error) {
	ret := _m.Called(ctx, backupID, key, overrideBucket, overridePath, w)

	if len(ret) == 0 {
		panic("no return value specified for Read")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, io.WriteCloser) (int64, error)); ok {
		return rf(ctx, backupID, key, overrideBucket, overridePath, w)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, io.WriteCloser) int64); ok {
		r0 = rf(ctx, backupID, key, overrideBucket, overridePath, w)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, string, string, io.WriteCloser) error); ok {
		r1 = rf(ctx, backupID, key, overrideBucket, overridePath, w)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBackupBackend_Read_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Read'
type MockBackupBackend_Read_Call struct {
	*mock.Call
}

// Read is a helper method to define mock.On call
//   - ctx context.Context
//   - backupID string
//   - key string
//   - overrideBucket string
//   - overridePath string
//   - w io.WriteCloser
func (_e *MockBackupBackend_Expecter) Read(ctx interface{}, backupID interface{}, key interface{}, overrideBucket interface{}, overridePath interface{}, w interface{}) *MockBackupBackend_Read_Call {
	return &MockBackupBackend_Read_Call{Call: _e.mock.On("Read", ctx, backupID, key, overrideBucket, overridePath, w)}
}

func (_c *MockBackupBackend_Read_Call) Run(run func(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, w io.WriteCloser)) *MockBackupBackend_Read_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].(io.WriteCloser))
	})
	return _c
}

func (_c *MockBackupBackend_Read_Call) Return(_a0 int64, _a1 error) *MockBackupBackend_Read_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBackupBackend_Read_Call) RunAndReturn(run func(context.Context, string, string, string, string, io.WriteCloser) (int64, error)) *MockBackupBackend_Read_Call {
	_c.Call.Return(run)
	return _c
}

// SourceDataPath provides a mock function with no fields
func (_m *MockBackupBackend) SourceDataPath() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for SourceDataPath")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockBackupBackend_SourceDataPath_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SourceDataPath'
type MockBackupBackend_SourceDataPath_Call struct {
	*mock.Call
}

// SourceDataPath is a helper method to define mock.On call
func (_e *MockBackupBackend_Expecter) SourceDataPath() *MockBackupBackend_SourceDataPath_Call {
	return &MockBackupBackend_SourceDataPath_Call{Call: _e.mock.On("SourceDataPath")}
}

func (_c *MockBackupBackend_SourceDataPath_Call) Run(run func()) *MockBackupBackend_SourceDataPath_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockBackupBackend_SourceDataPath_Call) Return(_a0 string) *MockBackupBackend_SourceDataPath_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_SourceDataPath_Call) RunAndReturn(run func() string) *MockBackupBackend_SourceDataPath_Call {
	_c.Call.Return(run)
	return _c
}

// Write provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath, r
func (_m *MockBackupBackend) Write(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, r io.ReadCloser) (int64, error) {
	ret := _m.Called(ctx, backupID, key, overrideBucket, overridePath, r)

	if len(ret) == 0 {
		panic("no return value specified for Write")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, io.ReadCloser) (int64, error)); ok {
		return rf(ctx, backupID, key, overrideBucket, overridePath, r)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, io.ReadCloser) int64); ok {
		r0 = rf(ctx, backupID, key, overrideBucket, overridePath, r)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, string, string, io.ReadCloser) error); ok {
		r1 = rf(ctx, backupID, key, overrideBucket, overridePath, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBackupBackend_Write_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Write'
type MockBackupBackend_Write_Call struct {
	*mock.Call
}

// Write is a helper method to define mock.On call
//   - ctx context.Context
//   - backupID string
//   - key string
//   - overrideBucket string
//   - overridePath string
//   - r io.ReadCloser
func (_e *MockBackupBackend_Expecter) Write(ctx interface{}, backupID interface{}, key interface{}, overrideBucket interface{}, overridePath interface{}, r interface{}) *MockBackupBackend_Write_Call {
	return &MockBackupBackend_Write_Call{Call: _e.mock.On("Write", ctx, backupID, key, overrideBucket, overridePath, r)}
}

func (_c *MockBackupBackend_Write_Call) Run(run func(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, r io.ReadCloser)) *MockBackupBackend_Write_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].(io.ReadCloser))
	})
	return _c
}

func (_c *MockBackupBackend_Write_Call) Return(_a0 int64, _a1 error) *MockBackupBackend_Write_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBackupBackend_Write_Call) RunAndReturn(run func(context.Context, string, string, string, string, io.ReadCloser) (int64, error)) *MockBackupBackend_Write_Call {
	_c.Call.Return(run)
	return _c
}

// WriteToFile provides a mock function with given fields: ctx, backupID, key, destPath, overrideBucket, overridePath
func (_m *MockBackupBackend) WriteToFile(ctx context.Context, backupID string, key string, destPath string, overrideBucket string, overridePath string) error {
	ret := _m.Called(ctx, backupID, key, destPath, overrideBucket, overridePath)

	if len(ret) == 0 {
		panic("no return value specified for WriteToFile")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, string) error); ok {
		r0 = rf(ctx, backupID, key, destPath, overrideBucket, overridePath)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockBackupBackend_WriteToFile_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'WriteToFile'
type MockBackupBackend_WriteToFile_Call struct {
	*mock.Call
}

// WriteToFile is a helper method to define mock.On call
//   - ctx context.Context
//   - backupID string
//   - key string
//   - destPath string
//   - overrideBucket string
//   - overridePath string
func (_e *MockBackupBackend_Expecter) WriteToFile(ctx interface{}, backupID interface{}, key interface{}, destPath interface{}, overrideBucket interface{}, overridePath interface{}) *MockBackupBackend_WriteToFile_Call {
	return &MockBackupBackend_WriteToFile_Call{Call: _e.mock.On("WriteToFile", ctx, backupID, key, destPath, overrideBucket, overridePath)}
}

func (_c *MockBackupBackend_WriteToFile_Call) Run(run func(ctx context.Context, backupID string, key string, destPath string, overrideBucket string, overridePath string)) *MockBackupBackend_WriteToFile_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].(string))
	})
	return _c
}

func (_c *MockBackupBackend_WriteToFile_Call) Return(_a0 error) *MockBackupBackend_WriteToFile_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockBackupBackend_WriteToFile_Call) RunAndReturn(run func(context.Context, string, string, string, string, string) error) *MockBackupBackend_WriteToFile_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockBackupBackend creates a new instance of MockBackupBackend. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockBackupBackend(t interface {
	mock.TestingT
	Cleanup(func())
},
) *MockBackupBackend {
	mock := &MockBackupBackend{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
