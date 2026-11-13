//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package diskio

import (
	"time"
)

type Reader interface {
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
}

type MeteredReaderCallback func(read int64, nanoseconds int64)

type MeteredReader struct {
	file Reader
	cb   MeteredReaderCallback
}

// Read passes the read through to the underlying reader and reports it to the
// callback; see [MeteredReader.observe].
func (m *MeteredReader) Read(p []byte) (n int, err error) {
	start := time.Now()
	n, err = m.file.Read(p)
	m.observe(n, err, time.Since(start))
	return n, err
}

// ReadAt passes the read through to the underlying reader and reports it to the
// callback; see [MeteredReader.observe].
func (m *MeteredReader) ReadAt(p []byte, off int64) (n int, err error) {
	start := time.Now()
	n, err = m.file.ReadAt(p, off)
	m.observe(n, err, time.Since(start))
	return n, err
}

// observe reports a read to the callback, if one is set. A failed read is
// reported only when it returned bytes, such as a short read that ends with
// io.EOF.
func (m *MeteredReader) observe(n int, err error, took time.Duration) {
	if m.cb == nil {
		return
	}
	if err != nil && n == 0 {
		return
	}
	m.cb(int64(n), took.Nanoseconds())
}

func NewMeteredReader(file Reader, cb MeteredReaderCallback) *MeteredReader {
	return &MeteredReader{file: file, cb: cb}
}
