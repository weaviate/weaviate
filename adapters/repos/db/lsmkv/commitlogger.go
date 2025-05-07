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

package lsmkv

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/integrity"
)

// commit log entry data format
// ---------------------------
// | version == 0 (1byte)    |
// | record (dynamic length) |
// ---------------------------

// ------------------------------------------------------
// | version == 1 (1byte)                               |
// | type (1byte)                                       |
// | node length (4bytes)                               |
// | node (dynamic length)                              |
// | checksum (crc32 4bytes non-checksum fields so far) |
// ------------------------------------------------------

type commitLogger struct {
	file   *os.File
	writer *bufWriter
	n      atomic.Int64
	path   string

	checksumWriter integrity.ChecksumWriter

	bufNode *bytes.Buffer
	tmpBuf  []byte

	// e.g. when recovering from an existing log, we do not want to write into a
	// new log again
	paused bool

	metrics *CommitLoggerMetrics
}

// CommitLoggerMetrics exposes metrics to understand IO happending at the WAL layer.
//
// Store ----> WAL ----> Filesystem page cache ----> Disk
//
// WAL does buffered IO. Meaning `Write()` fill the buffer and `Flush()` actually does the IO by writing to Filesystem page cache and eventually `Fsync()` persist to disk.
// Given the expensive nature of `fsync()` and the fact internally Operating System does periodic `fsync()` to transfer bytes from filesystem cache into disk, at the application layer we do `fsync()` only during closing the underlying WAL segment file.
type CommitLoggerMetrics struct {
	// writtenEntriesSize gives visibility on size distribution of write payload coming to commitLogger via `Write()`.
	// It also gives visibility on IOPS and Througput of the WAL at the application layer.
	writtenEntriesSize prometheus.Histogram

	// flushedEntriesSize gives visibility on size distribution of actual pages being `Flush()`ed.
	// Ideally this should all be PageSize of the underlying buffered writed.
	flushedEntriesSize prometheus.Histogram

	fsyncDuration prometheus.Histogram
}

func NewCommitLoggerMetrics(reg prometheus.Registerer) *CommitLoggerMetrics {
	r := promauto.With(reg)
	return &CommitLoggerMetrics{
		writtenEntriesSize: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_commitlogger_written_entries_bytes",
			Help:    "Size distribution of each entry written by commitLogger",
			Buckets: prometheus.ExponentialBuckets(1024, 4, 6), // 1K, 4K, 16K, 64K, 256K, 1M
		}),
		flushedEntriesSize: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_commitlogger_flushed_entries_bytes",
			Help:    "Size distribution of each entry flushed to disk by commitLogger",
			Buckets: prometheus.ExponentialBuckets(1024, 4, 6), // 1K, 4K, 16K, 64K, 256K, 1M
		}),
		fsyncDuration: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_commitlogger_fsync_duration_seconds",
			Help:    "Time distribution of each fsync call made by commitLogger",
			Buckets: prometheus.ExponentialBuckets(0.01, 10, 4), // 10ms, 100ms, 1s, 10s
		}),
	}
}

const (
	defaultPageSize = 4 * 1 << 10 // 4KiB
)

type bufWriter struct {
	buf      *bytes.Buffer
	w        io.Writer
	pageSize int

	// metrics
	writtenSize     prometheus.Histogram
	pageFlushedSize prometheus.Histogram
}

func newbufWriter(size int, w io.Writer, writtenSize, pageFlushedSize prometheus.Histogram) *bufWriter {
	return &bufWriter{
		pageSize:        size,
		w:               w,
		buf:             &bytes.Buffer{},
		writtenSize:     writtenSize,
		pageFlushedSize: pageFlushedSize,
	}
}

func (b *bufWriter) Write(p []byte) (int, error) {
	n, err := b.buf.Write(p)
	if err != nil {
		return n, err
	}
	if b.buf.Len() > b.pageSize {
		if err := b.Flush(); err != nil {
			return n, err
		}
	}

	b.writtenSize.Observe(float64(n))

	return n, nil
}

func (b *bufWriter) Flush() error {
	n, err := b.w.Write(b.buf.Bytes())
	if err != nil {
		return err
	}

	b.pageFlushedSize.Observe(float64(n))

	return nil
}

const CurrentCommitLogVersion uint8 = 1

type CommitType uint8

const (
	CommitTypeReplace CommitType = iota // replace strategy

	// collection strategy - this can handle all cases as updates and deletes are
	// only appends in a collection strategy
	CommitTypeCollection
	CommitTypeRoaringSet
	// new version of roaringset that stores data as a list of uint64 values,
	// instead of a roaring bitmap
	CommitTypeRoaringSetList
)

func (ct CommitType) String() string {
	switch ct {
	case CommitTypeReplace:
		return "replace"
	case CommitTypeCollection:
		return "collection"
	case CommitTypeRoaringSet:
		return "roaringset"
	case CommitTypeRoaringSetList:
		return "roaringsetlist"
	default:
		return "unknown"
	}
}

func (ct CommitType) Is(checkedCommitType CommitType) bool {
	return ct == checkedCommitType
}

func newCommitLogger(path string, strategy string, metrics *CommitLoggerMetrics) (*commitLogger, error) {
	out := &commitLogger{
		path:    path + ".wal",
		metrics: metrics,
	}

	f, err := os.OpenFile(out.path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"strategy":  strategy,
		"operation": "appendWAL",
	})

	out.file = f

	meteredF := diskio.NewMeteredWriter(f, func(written int64) {
		observeWrite.Observe(float64(written))
	})

	out.writer = newbufWriter(defaultPageSize, meteredF, out.metrics.writtenEntriesSize, out.metrics.flushedEntriesSize)
	out.checksumWriter = integrity.NewCRC32Writer(out.writer)

	out.bufNode = bytes.NewBuffer(nil)
	out.tmpBuf = make([]byte, byteops.Uint8Len+byteops.Uint8Len+byteops.Uint32Len)

	return out, nil
}

func (cl *commitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?

	rw := byteops.NewReadWriter(cl.tmpBuf)
	rw.WriteByte(byte(commitType))
	rw.WriteByte(CurrentCommitLogVersion)
	rw.WriteUint32(uint32(len(nodeBytes)))

	_, err := cl.checksumWriter.Write(rw.Buffer)
	if err != nil {
		return err
	}

	_, err = cl.checksumWriter.Write(nodeBytes)
	if err != nil {
		return err
	}

	// write record checksum directly on the writer
	checksumSize, err := cl.writer.Write(cl.checksumWriter.Hash())
	if err != nil {
		return err
	}

	cl.n.Add(int64(1 + 1 + 4 + len(nodeBytes) + checksumSize))

	return nil
}

func (cl *commitLogger) put(node segmentReplaceNode) error {
	if cl.paused {
		return nil
	}

	cl.bufNode.Reset()

	ki, err := node.KeyIndexAndWriteTo(cl.bufNode)
	if err != nil {
		return err
	}
	if len(cl.bufNode.Bytes()) != ki.ValueEnd-ki.ValueStart {
		return fmt.Errorf("unexpected error, node size mismatch")
	}

	return cl.writeEntry(CommitTypeReplace, cl.bufNode.Bytes())
}

func (cl *commitLogger) append(node segmentCollectionNode) error {
	if cl.paused {
		return nil
	}

	cl.bufNode.Reset()

	ki, err := node.KeyIndexAndWriteTo(cl.bufNode)
	if err != nil {
		return err
	}
	if len(cl.bufNode.Bytes()) != ki.ValueEnd-ki.ValueStart {
		return fmt.Errorf("unexpected error, node size mismatch")
	}

	return cl.writeEntry(CommitTypeCollection, cl.bufNode.Bytes())
}

func (cl *commitLogger) add(node *roaringset.SegmentNodeList) error {
	if cl.paused {
		return nil
	}

	cl.bufNode.Reset()

	ki, err := node.KeyIndexAndWriteTo(cl.bufNode, 0)
	if err != nil {
		return err
	}
	if len(cl.bufNode.Bytes()) != ki.ValueEnd-ki.ValueStart {
		return fmt.Errorf("unexpected error, node size mismatch")
	}

	return cl.writeEntry(CommitTypeRoaringSetList, cl.bufNode.Bytes())
}

// Size returns the amount of data that has been written since the commit
// logger was initialized. After a flush a new logger is initialized which
// automatically resets the logger.
func (cl *commitLogger) Size() int64 {
	return cl.n.Load()
}

func (cl *commitLogger) close() error {
	if !cl.paused {
		if err := cl.writer.Flush(); err != nil {
			return err
		}

		start := time.Now()
		if err := cl.file.Sync(); err != nil {
			return err
		}
		cl.metrics.fsyncDuration.Observe(float64(time.Since(start)))
	}

	return cl.file.Close()
}

func (cl *commitLogger) pause() {
	cl.paused = true
}

func (cl *commitLogger) unpause() {
	cl.paused = false
}

func (cl *commitLogger) delete() error {
	return os.Remove(cl.path)
}

func (cl *commitLogger) flushBuffers() error {
	err := cl.writer.Flush()
	if err != nil {
		return fmt.Errorf("flushing WAL %q: %w", cl.path, err)
	}

	return nil
}
