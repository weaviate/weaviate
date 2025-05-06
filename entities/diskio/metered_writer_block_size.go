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

package diskio

import (
	"io"
)

type MeteredWriterCallback func(written int64)

type WriterSeekerCloser interface {
	io.Writer
	io.Seeker
}

type MeteredWriter struct {
	w  WriterSeekerCloser
	cb MeteredWriterCallback
}

func (m *MeteredWriter) Write(p []byte) (n int, err error) {
	n, err = m.w.Write(p)
	if err != nil {
		return
	}

	if m.cb != nil {
		m.cb(int64(n))
	}

	return
}

func (m *MeteredWriter) Seek(offset int64, whence int) (int64, error) {
	n, err := m.w.Seek(offset, whence)
	if m.cb != nil {
		m.cb(0)
	}

	return n, err
}

var _ = WriterSeekerCloser(&MeteredWriter{})

func NewMeteredWriter(w WriterSeekerCloser, cb MeteredWriterCallback) *MeteredWriter {
	return &MeteredWriter{
		w:  w,
		cb: cb,
	}
}
