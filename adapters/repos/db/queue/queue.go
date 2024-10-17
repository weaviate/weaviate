//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviatq.io
//

package queue

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// defaultChunkSize is the maximum size of each chunk file.
	// It is set to 144KB to be a multiple of 4KB (common page size)
	// and of 9 bytes (size of a single record).
	// This also works for larger page sizes (e.g. 16KB for macOS).
	// The goal is to have a quick way of determining the number of records
	// without having to read the entire file or to maintain an index.
	defaultChunkSize = 144 * 1024

	// name of the file that stores records before they reach the target size.
	partialChunkFile = "chunk.bin.partial"
	// chunkFileFmt is the format string for the promoted chunk files,
	// i.e. files that have reached the target size, or those that are
	// stale and need to be promoted.
	chunkFileFmt = "chunk-%d.bin"
)

type Queue interface {
	ID() string
	Size() int64
	DecodeBatch(dec *Decoder) (batch []Task, done func(), err error)
}

type BeforeScheduleHook interface {
	BeforeSchedule() bool
}

type DiskQueue struct {
	// Logger for the queue. Wrappers of this queue should use this logger.
	Logger logrus.FieldLogger
	// BeforeScheduleFn is a hook that is called before the queue is scheduled.
	BeforeScheduleFn func() (skip bool)

	scheduler        *Scheduler
	id               string
	dir              string
	lastPushTime     atomic.Pointer[time.Time]
	closed           atomic.Bool
	chunkSize        int
	m                sync.RWMutex
	w                bufio.Writer
	f                *os.File
	partialChunkSize int
	recordCount      int64
}

func New(s *Scheduler, logger logrus.FieldLogger, id, dir string) (*DiskQueue, error) {
	logger = logger.WithField("queue_id", id)

	q := DiskQueue{
		Logger:    logger,
		scheduler: s,
		id:        id,
		dir:       dir,
		chunkSize: defaultChunkSize,
	}

	// create the directory if it doesn't exist
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	// determine the number of records stored on disk
	q.recordCount, err = q.calculateRecordCount()
	if err != nil {
		return nil, err
	}

	s.RegisterQueue(&q)

	return &q, nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *DiskQueue) Close() error {
	if q.closed.Swap(true) {
		return errors.New("queue already closed")
	}

	q.scheduler.UnregisterQueue(q.id)

	err := q.Flush()
	if err != nil {
		return err
	}

	q.m.Lock()
	defer q.m.Unlock()

	if q.f != nil {
		err := q.f.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk")
		}

		q.f = nil
	}

	return nil
}

func (q *DiskQueue) ID() string {
	return q.id
}

func (q *DiskQueue) Push(r *Record) error {
	if q.closed.Load() {
		return errors.New("queue closed")
	}

	now := time.Now()
	q.lastPushTime.Store(&now)

	q.m.Lock()
	defer q.m.Unlock()

	err := q.ensureChunk()
	if err != nil {
		return err
	}

	n, err := r.buf.WriteTo(&q.w)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	q.partialChunkSize += int(n)
	q.recordCount++

	return nil
}

func (q *DiskQueue) ensureChunk() error {
	if q.f != nil && q.partialChunkSize+9 /* size of op + key */ > q.chunkSize {
		err := q.promoteChunkNoLock()
		if err != nil {
			return err
		}
	}

	if q.f == nil {
		// create or open partial chunk
		path := filepath.Join(q.dir, partialChunkFile)
		// open append mode, write only
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return errors.Wrap(err, "failed to create chunk file")
		}

		q.w.Reset(f)
		q.f = f

		// get file size
		info, err := f.Stat()
		if err != nil {
			return errors.Wrap(err, "failed to stat chunk file")
		}

		q.partialChunkSize = int(info.Size())
	}

	return nil
}

func (q *DiskQueue) promoteChunkNoLock() error {
	if q.f == nil {
		return nil
	}

	fName := q.f.Name()

	// flush and close current chunk
	err := q.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush chunk")
	}
	err = q.f.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync chunk")
	}
	err = q.f.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close chunk")
	}

	q.f = nil
	q.partialChunkSize = 0

	// rename the file to remove the .partial suffix
	// and add a timestamp to the filename
	newPath := filepath.Join(q.dir, fmt.Sprintf(chunkFileFmt, time.Now().UnixNano()))
	err = os.Rename(fName, newPath)
	if err != nil {
		return errors.Wrap(err, "failed to rename chunk file")
	}

	q.Logger.WithField("file", newPath).Debug("chunk file created")

	return nil
}

// This method is used by the scheduler to promote the current chunk
// when it's been stalled for too long.
func (q *DiskQueue) promoteChunk() error {
	q.m.Lock()
	defer q.m.Unlock()

	return q.promoteChunkNoLock()
}

func (q *DiskQueue) Flush() error {
	q.m.Lock()
	defer q.m.Unlock()

	err := q.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush")
	}

	if q.f == nil {
		return nil
	}

	return q.f.Sync()
}

func (q *DiskQueue) Size() int64 {
	q.m.RLock()
	defer q.m.RUnlock()

	return q.recordCount
}

func (q *DiskQueue) Pause() {
	q.scheduler.PauseQueue(q.id)
}

func (q *DiskQueue) Resume() {
	q.scheduler.ResumeQueue(q.id)
}

func (q *DiskQueue) Wait() {
	q.scheduler.Wait(q.id)
}

func (q *DiskQueue) Drop() error {
	_ = q.Close()

	q.m.Lock()
	defer q.m.Unlock()

	if q.f != nil {
		err := q.f.Close()
		if err != nil {
			q.Logger.WithError(err).Error("failed to close chunk")
		}
	}

	// remove the directory
	err := os.RemoveAll(q.dir)
	if err != nil {
		return errors.Wrap(err, "failed to remove directory")
	}

	return nil
}

// calculateRecordCount is a slow method that determines the number of records
// stored on disk and in the partial chunk, by reading the size of all the files in the directory.
// It is used when the encoder is first initialized.
func (q *DiskQueue) calculateRecordCount() (int64, error) {
	q.m.Lock()
	defer q.m.Unlock()

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read directory")
	}

	var size int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if entry.Name() != partialChunkFile && !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return 0, errors.Wrap(err, "failed to get file info")
		}

		size += int64(info.Size()) / 9 /* size of a single record */
	}

	return size, nil
}

func (q *DiskQueue) removeChunk(path string) {
	q.m.Lock()
	defer q.m.Unlock()

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}

		q.Logger.WithError(err).WithField("file", path).Warn("failed to stat chunk. trying to remove it anyway")
	}

	err = os.Remove(path)
	if err != nil {
		q.Logger.WithError(err).WithField("file", path).Error("failed to remove chunk")
		return
	}

	q.recordCount -= int64(info.Size()) / 9

	q.Logger.WithField("file", path).Debug("chunk removed")
}

func (q *DiskQueue) BeforeSchedule() bool {
	if q.BeforeScheduleFn != nil {
		return q.BeforeScheduleFn()
	}

	return false
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type Record struct {
	*msgpack.Encoder
	buf *bytes.Buffer
}

func NewRecord() *Record {
	enc := msgpack.GetEncoder()
	buf := bufPool.Get().(*bytes.Buffer)
	enc.Reset(buf)

	return &Record{
		Encoder: enc,
		buf:     buf,
	}
}

func (r *Record) Release() {
	msgpack.PutEncoder(r.Encoder)
	r.buf.Reset()
	bufPool.Put(r.buf)
}

func (r *Record) Reset() {
	r.buf.Reset()
	r.Encoder.Reset(r.buf)
}

// shadow most msgpack methods with functions that don't return errors
func (r *Record) EncodeInt8(n int8) {
	r.Encoder.EncodeInt8(n)
}

func (r *Record) EncodeUint8(n uint8) {
	r.Encoder.EncodeUint8(n)
}

func (r *Record) EncodeUint16(n uint16) {
	r.Encoder.EncodeUint16(n)
}

func (r *Record) EncodeUint32(n uint32) {
	r.Encoder.EncodeUint32(n)
}

func (r *Record) EncodeUint64(n uint64) {
	r.Encoder.EncodeUint64(n)
}

func (r *Record) EncodeInt16(n int16) {
	r.Encoder.EncodeInt16(n)
}

func (r *Record) EncodeInt32(n int32) {
	r.Encoder.EncodeInt32(n)
}

func (r *Record) EncodeInt64(n int64) {
	r.Encoder.EncodeInt64(n)
}

func (r *Record) EncodeFloat32(n float32) {
	r.Encoder.EncodeFloat32(n)
}

func (r *Record) EncodeFloat64(n float64) {
	r.Encoder.EncodeFloat64(n)
}

func (r *Record) EncodeString(s string) {
	r.Encoder.EncodeString(s)
}

func (r *Record) EncodeBytes(b []byte) {
	r.Encoder.EncodeBytes(b)
}

func (r *Record) EncodeNil() {
	r.Encoder.EncodeNil()
}

func (r *Record) EncodeBool(b bool) {
	r.Encoder.EncodeBool(b)
}

func (r *Record) EncodeTime(t time.Time) {
	r.Encoder.EncodeTime(t)
}

func (r *Record) EncodeArrayLen(n int) {
	r.Encoder.EncodeArrayLen(n)
}

func (r *Record) EncodeMapLen(n int) {
	r.Encoder.EncodeMapLen(n)
}

type Decoder struct {
	*msgpack.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	dec := msgpack.GetDecoder()
	dec.Reset(r)

	return &Decoder{
		dec,
	}
}

func (d *Decoder) DecodeOp() (uint8, error) {
	n, err := d.DecodeInt8()
	return uint8(n), err
}

func (d *Decoder) DecodeFloat32Array() ([]float32, error) {
	n, err := d.DecodeArrayLen()
	if err != nil {
		return nil, err
	}

	vec := make([]float32, n)
	for i := 0; i < n; i++ {
		v, err := d.DecodeFloat32()
		if err != nil {
			return nil, err
		}

		vec[i] = v
	}

	return vec, nil
}
