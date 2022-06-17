package diskio

import (
	"io"
	"time"
)

type MeteredReaderCallback func(read int64, nanoseconds int64)

type MeteredReader struct {
	r  io.Reader
	cb MeteredReaderCallback
}

// Read passes the read through to the underlying reader. On a succesful read,
// it will trigger the attached callback and provide it with metrics. If no
// callback is set, it will ignore it.
func (m *MeteredReader) Read(p []byte) (n int, err error) {
	start := time.Now()
	n, err = m.r.Read(p)
	took := time.Since(start).Nanoseconds()
	if err != nil {
		return
	}

	if m.cb != nil {
		m.cb(int64(n), took)
	}

	return
}

func NewMeteredReader(r io.Reader, cb MeteredReaderCallback) *MeteredReader {
	return &MeteredReader{r: r, cb: cb}
}
