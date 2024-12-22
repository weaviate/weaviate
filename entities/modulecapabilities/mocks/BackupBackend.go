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

import (
	context "context"

	backup "github.com/liutizhong/weaviate/entities/backup"

	io "io"

	mock "github.com/stretchr/testify/mock"
)

// BackupBackend is an autogenerated mock type for the BackupBackend type
type BackupBackend struct {
	mock.Mock
}

// AllBackups provides a mock function with given fields: ctx
func (_m *BackupBackend) AllBackups(ctx context.Context) ([]*backup.DistributedBackupDescriptor, error) {
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

// GetObject provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath
func (_m *BackupBackend) GetObject(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string) ([]byte, error) {
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

// HomeDir provides a mock function with given fields: backupID, overrideBucket, overridePath
func (_m *BackupBackend) HomeDir(backupID string, overrideBucket string, overridePath string) string {
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

// Initialize provides a mock function with given fields: ctx, backupID, overrideBucket, overridePath
func (_m *BackupBackend) Initialize(ctx context.Context, backupID string, overrideBucket string, overridePath string) error {
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

// IsExternal provides a mock function with given fields:
func (_m *BackupBackend) IsExternal() bool {
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

// Name provides a mock function with given fields:
func (_m *BackupBackend) Name() string {
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

// PutObject provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath, byes
func (_m *BackupBackend) PutObject(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, byes []byte) error {
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

// Read provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath, w
func (_m *BackupBackend) Read(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, w io.WriteCloser) (int64, error) {
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

// SourceDataPath provides a mock function with given fields:
func (_m *BackupBackend) SourceDataPath() string {
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

// Write provides a mock function with given fields: ctx, backupID, key, overrideBucket, overridePath, r
func (_m *BackupBackend) Write(ctx context.Context, backupID string, key string, overrideBucket string, overridePath string, r io.ReadCloser) (int64, error) {
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

// WriteToFile provides a mock function with given fields: ctx, backupID, key, destPath, overrideBucket, overridePath
func (_m *BackupBackend) WriteToFile(ctx context.Context, backupID string, key string, destPath string, overrideBucket string, overridePath string) error {
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

// NewBackupBackend creates a new instance of BackupBackend. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBackupBackend(t interface {
	mock.TestingT
	Cleanup(func())
}) *BackupBackend {
	mock := &BackupBackend{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
