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
	MkdirAll(path string, perm os.FileMode) error
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	ReadDir(name string) ([]os.DirEntry, error)
	Stat(name string) (os.FileInfo, error)
	Remove(name string) error
	RemoveAll(path string) error
	Create(name string) (File, error)
	Open(name string) (File, error)
	Rename(oldpath, newpath string) error
	Truncate(name string, size int64) error
}

type osFS struct{}

func NewOSFS() FS {
	return &osFS{}
}

func (fs *osFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
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

func (fs *osFS) Create(name string) (File, error) {
	return os.Create(name)
}

func (fs *osFS) Open(name string) (File, error) {
	return os.Open(name)
}

func (fs *osFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *osFS) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}
