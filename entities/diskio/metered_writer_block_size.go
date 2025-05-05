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

// func (m *MeteredWriter) Close() error {
// 	return m.w.Close()
// }

var _ = WriterSeekerCloser(&MeteredWriter{})

func NewMeteredWriter(w WriterSeekerCloser, cb MeteredWriterCallback) *MeteredWriter {
	return &MeteredWriter{
		w:  w,
		cb: cb,
	}
}
