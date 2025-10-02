//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"io"
	"os"
)

var (
	_ File = (*os.File)(nil)
	_ FS   = (*osFS)(nil)
)

type File interface {
	io.Reader
	io.Writer
	io.Closer
	io.ReaderAt
	io.Seeker
	Sync() error
	Stat() (os.FileInfo, error)
}

type FS interface {
	Open(name string) (File, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Create(name string) (File, error)
	MkdirAll(path string, perm os.FileMode) error
	ReadDir(name string) ([]os.DirEntry, error)
	Stat(name string) (os.FileInfo, error)
	Remove(name string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	Truncate(name string, size int64) error
}

type osFS struct{}

func NewOSFS() FS {
	return &osFS{}
}

func (fs *osFS) Create(name string) (File, error) {
	return os.Create(name)
}

func (fs *osFS) Open(name string) (File, error) {
	return os.Open(name)
}

func (fs *osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *osFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *osFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (fs *osFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *osFS) Remove(name string) error {
	return os.Remove(name)
}

func (fs *osFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *osFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *osFS) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

type TestFS struct {
	FS
	OnOpenFile func(f File) File
	OnOpen     func(f File) File
	OnCreate   func(f File) File
	OnRename   func(oldpath, newpath string) error
	OnRemove   func(name string) error
}

func NewTestFS() *TestFS {
	return &TestFS{
		FS: NewOSFS(),
	}
}

func (fs *TestFS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	if fs.OnOpen == nil {
		return f, nil
	}
	return fs.OnOpen(f), nil
}

func (fs *TestFS) Create(name string) (File, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	if fs.OnCreate == nil {
		return f, nil
	}
	return fs.OnCreate(f), nil
}

func (fs *TestFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if fs.OnOpenFile == nil {
		return f, nil
	}

	return fs.OnOpenFile(f), nil
}

func (fs *TestFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (fs *TestFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *TestFS) Remove(name string) error {
	if fs.OnRemove != nil {
		return fs.OnRemove(name)
	}
	return os.Remove(name)
}

func (fs *TestFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *TestFS) Rename(oldpath, newpath string) error {
	if fs.OnRename != nil {
		return fs.OnRename(oldpath, newpath)
	}
	return os.Rename(oldpath, newpath)
}

func (fs *TestFS) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

type TestFile struct {
	File
	OnWrite func(b []byte) (n int, err error)
	OnRead  func(b []byte) (n int, err error)
	OnSync  func() error
}

func (f *TestFile) Write(b []byte) (n int, err error) {
	if f.OnWrite != nil {
		return f.OnWrite(b)
	}
	return f.File.Write(b)
}

func (f *TestFile) Read(b []byte) (n int, err error) {
	if f.OnRead != nil {
		return f.OnRead(b)
	}
	return f.File.Read(b)
}

func (f *TestFile) Sync() error {
	if f.OnSync != nil {
		return f.OnSync()
	}
	return f.File.Sync()
}
