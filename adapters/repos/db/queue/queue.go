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

package queue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// defaultChunkSize is the maximum size of each chunk file. Defaults to 10MB.
	defaultChunkSize = 10 * 1024 * 1024

	// defaultStaleTimeout is the duration after which a partial chunk is considered stale.
	// If no tasks are pushed to the queue for this duration, the partial chunk is scheduled.
	defaultStaleTimeout = 100 * time.Millisecond

	// chunkWriterBufferSize is the size of the buffer used by the chunk writer.
	// It should be large enough to hold a few records, but not too large to avoid
	// taking up too much memory when the number of queues is large.
	chunkWriterBufferSize = 256 * 1024

	// chunkFileFmt is the format string for the chunk files,
	chunkFileFmt = "chunk-%d.bin"

	magicHeader = "WV8Q"
)

// regex pattern for the chunk files
var chunkFilePattern = regexp.MustCompile(`chunk-\d+\.bin`)

type Queue interface {
	ID() string
	Size() int64
	DequeueBatch() (batch *Batch, err error)
	Metrics() *Metrics
}

type BeforeScheduleHook interface {
	BeforeSchedule() bool
}

type DiskQueue struct {
	// Logger for the queue. Wrappers of this queue should use this logger.
	Logger           logrus.FieldLogger
	staleTimeout     time.Duration
	taskDecoder      TaskDecoder
	scheduler        *Scheduler
	id               string
	dir              string
	onBatchProcessed func()
	metrics          *Metrics
	chunkSize        uint64

	// m protects the disk operations
	m            sync.RWMutex
	lastPushTime time.Time
	closed       bool
	w            *chunkWriter
	r            *chunkReader
	recordCount  uint64
	diskUsage    int64

	rmLock sync.Mutex
}

type DiskQueueOptions struct {
	// Required
	ID          string
	Scheduler   *Scheduler
	Dir         string
	TaskDecoder TaskDecoder

	// Optional
	Logger           logrus.FieldLogger
	StaleTimeout     time.Duration
	ChunkSize        uint64
	OnBatchProcessed func()
	Metrics          *Metrics
}

func NewDiskQueue(opt DiskQueueOptions) (*DiskQueue, error) {
	if opt.ID == "" {
		return nil, errors.New("id is required")
	}
	if opt.Scheduler == nil {
		return nil, errors.New("scheduler is required")
	}
	if opt.Dir == "" {
		return nil, errors.New("dir is required")
	}
	if opt.TaskDecoder == nil {
		return nil, errors.New("task decoder is required")
	}
	if opt.Logger == nil {
		opt.Logger = logrus.New()
	}
	opt.Logger = opt.Logger.
		WithField("queue_id", opt.ID).
		WithField("action", "disk_queue")

	if opt.Metrics == nil {
		opt.Metrics = NewMetrics(opt.Logger, nil, nil)
	}
	if opt.StaleTimeout <= 0 {
		opt.StaleTimeout = defaultStaleTimeout
	}
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = defaultChunkSize
	}

	q := DiskQueue{
		id:               opt.ID,
		scheduler:        opt.Scheduler,
		dir:              opt.Dir,
		Logger:           opt.Logger,
		staleTimeout:     opt.StaleTimeout,
		taskDecoder:      opt.TaskDecoder,
		metrics:          opt.Metrics,
		onBatchProcessed: opt.OnBatchProcessed,
		chunkSize:        opt.ChunkSize,
	}

	return &q, nil
}

func (q *DiskQueue) Init() error {
	// create the directory if it doesn't exist
	err := os.MkdirAll(q.dir, 0o755)
	if err != nil {
		return errors.Wrap(err, "failed to create directory")
	}

	// determine the number of records stored on disk
	// and the disk usage
	chunkList, err := q.analyzeDisk()
	if err != nil {
		return err
	}

	// create chunk reader
	q.r = newChunkReader(q.dir, chunkList)

	// create chunk writer
	q.w, err = newChunkWriter(q.dir, q.r, q.Logger, q.chunkSize)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk writer")
	}
	q.recordCount += q.w.recordCount

	// set the last push time to now
	q.lastPushTime = time.Now()

	return nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *DiskQueue) Close() error {
	if q == nil {
		return nil
	}

	q.m.Lock()
	if q.closed {
		q.m.Unlock()
		return errors.New("queue already closed")
	}
	q.closed = true
	q.m.Unlock()

	q.scheduler.UnregisterQueue(q.id)

	q.m.Lock()
	defer q.m.Unlock()

	if q.w != nil {
		err := q.w.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk writer")
		}
	}

	if q.r != nil {
		err := q.r.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk reader")
		}
	}

	return nil
}

func (q *DiskQueue) Metrics() *Metrics {
	return q.metrics
}

func (q *DiskQueue) ID() string {
	return q.id
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func (q *DiskQueue) Push(record []byte) error {
	q.m.RLock()
	if q.closed {
		q.m.RUnlock()
		return errors.New("queue closed")
	}
	q.m.RUnlock()

	if len(record) == 0 {
		return errors.New("empty record")
	}

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	buf.Reset()

	var bytesBuf [4]byte
	// length of the record in 4 bytes
	binary.BigEndian.PutUint32(bytesBuf[:], uint32(len(record)))
	_, err := buf.Write(bytesBuf[:])
	if err != nil {
		return errors.Wrap(err, "failed to write record length")
	}

	// write the record
	_, err = buf.Write(record)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	q.m.Lock()
	defer q.m.Unlock()

	q.lastPushTime = time.Now()

	n, err := q.w.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	q.recordCount++
	q.diskUsage += int64(n)

	return nil
}

func (q *DiskQueue) Scheduler() *Scheduler {
	return q.scheduler
}

func (q *DiskQueue) Flush() error {
	q.m.Lock()
	defer q.m.Unlock()

	return q.w.Flush()
}

func (q *DiskQueue) DequeueBatch() (batch *Batch, err error) {
	c, err := q.r.ReadChunk()
	if err != nil {
		return nil, err
	}

	// if there are no more chunks to read,
	// check if the partial chunk is stale (e.g no tasks were pushed for a while)
	if c == nil || c.f == nil {
		c, err = q.checkIfStale()
		if c == nil || err != nil || c.f == nil {
			return nil, err
		}
	}

	if c.f == nil {
		return nil, nil
	}
	defer c.Close()

	// decode all tasks from the chunk
	// and partition them by worker
	tasks := make([]Task, 0, c.count)

	buf := make([]byte, 4)
	for {
		buf = buf[:4]

		// read the record length
		_, err := io.ReadFull(c.r, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to read record length")
		}
		length := binary.BigEndian.Uint32(buf)
		if length == 0 {
			return nil, errors.New("invalid record length")
		}

		// read the record
		if cap(buf) < int(length) {
			buf = make([]byte, length)
		} else {
			buf = buf[:length]
		}
		_, err = io.ReadFull(c.r, buf)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read record")
		}

		// decode the task
		t, err := q.taskDecoder.DecodeTask(buf)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode task")
		}

		tasks = append(tasks, t)
	}

	err = c.Close()
	if err != nil {
		q.Logger.WithField("file", c.path).WithError(err).Warn("failed to close chunk file")
	}

	if len(tasks) == 0 {
		// empty chunk, remove it
		q.removeChunk(c)
		return nil, nil
	}

	doneFn := func() {
		q.removeChunk(c)
		if q.onBatchProcessed != nil {
			q.onBatchProcessed()
		}
	}

	return &Batch{
		Tasks:  tasks,
		onDone: doneFn,
	}, nil
}

func (q *DiskQueue) checkIfStale() (*chunk, error) {
	if q.Size() == 0 {
		return nil, nil
	}

	q.m.Lock()

	if q.w.f == nil {
		q.m.Unlock()
		return nil, nil
	}

	if q.w.recordCount == 0 {
		q.m.Unlock()
		return nil, nil
	}

	if time.Since(q.lastPushTime) < q.staleTimeout {
		q.m.Unlock()
		return nil, nil
	}

	q.Logger.Debug("partial chunk is stale, scheduling")

	err := q.w.Promote()
	if err != nil {
		q.m.Unlock()
		return nil, err
	}

	q.m.Unlock()

	return q.r.ReadChunk()
}

func (q *DiskQueue) Size() int64 {
	if q == nil {
		return 0
	}

	q.m.RLock()
	defer q.m.RUnlock()

	q.metrics.Size(q.recordCount)
	q.metrics.DiskUsage(q.diskUsage)
	return int64(q.recordCount)
}

func (q *DiskQueue) Pause() {
	q.scheduler.PauseQueue(q.id)
	q.metrics.Paused(q.id)
}

func (q *DiskQueue) Resume() {
	q.scheduler.ResumeQueue(q.id)
	q.metrics.Resumed(q.id)
}

func (q *DiskQueue) Wait() {
	q.scheduler.Wait(q.id)
}

func (q *DiskQueue) Drop() error {
	if q == nil {
		return nil
	}

	err := q.Close()
	if err != nil {
		q.Logger.WithError(err).Error("failed to close queue")
	}

	q.m.Lock()
	defer q.m.Unlock()

	// remove the directory
	err = os.RemoveAll(q.dir)
	if err != nil {
		return errors.Wrap(err, "failed to remove directory")
	}

	return nil
}

func (q *DiskQueue) removeChunk(c *chunk) {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	deleted, err := q.r.RemoveChunk(c)
	if err != nil {
		q.Logger.WithError(err).WithField("file", c.path).Error("failed to remove chunk")
		return
	}
	if !deleted {
		return
	}

	q.m.Lock()
	q.recordCount -= c.count
	q.diskUsage -= int64(c.size)
	q.metrics.DiskUsage(q.diskUsage)
	q.metrics.Size(q.recordCount)
	q.m.Unlock()
}

// analyzeDisk is a slow method that determines the number of records
// stored on disk and in the partial chunk by reading the header of all the files in the directory.
// It also calculates the disk usage.
// It is used when the queue is first initialized.
func (q *DiskQueue) analyzeDisk() ([]string, error) {
	q.m.Lock()
	defer q.m.Unlock()

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read directory")
	}

	chunkList := make([]string, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// check if the entry name matches the regex pattern of a chunk file
		if !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		fi, err := entry.Info()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get file info")
		}

		filePath := filepath.Join(q.dir, entry.Name())

		if fi.Size() == 0 {
			// best effort to remove empty files
			_ = os.Remove(filePath)
			continue
		}

		q.diskUsage += fi.Size()

		count, err := q.readChunkRecordCount(filePath)
		if err != nil {
			return nil, err
		}

		// partial chunk
		if count == 0 {
			continue
		}

		q.recordCount += count

		chunkList = append(chunkList, filePath)
		continue
	}

	return chunkList, nil
}

func (q *DiskQueue) readChunkRecordCount(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return readChunkHeader(f)
}

var readerPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, defaultChunkSize)
	},
}

type chunk struct {
	path  string
	r     *bufio.Reader
	f     *os.File
	count uint64
	size  uint64
}

func openChunk(path string) (*chunk, error) {
	var err error
	c := chunk{
		path: path,
	}

	c.f, err = os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := c.f.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		// empty file
		// remove it
		err = c.f.Close()
		if err != nil {
			return nil, err
		}

		err = os.Remove(path)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	c.r = readerPool.Get().(*bufio.Reader)
	c.r.Reset(c.f)
	c.size = uint64(stat.Size())

	// check the header
	c.count, err = readChunkHeader(c.r)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func chunkFromFile(f *os.File) (*chunk, error) {
	var err error
	c := chunk{
		path: f.Name(),
		f:    f,
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	c.r = readerPool.Get().(*bufio.Reader)
	c.r.Reset(c.f)

	// check the header
	c.count, err = readChunkHeader(c.r)
	if err != nil {
		return nil, err
	}

	// get the file size
	info, err := c.f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat chunk file")
	}

	c.size = uint64(info.Size())

	return &c, nil
}

func (c *chunk) Close() error {
	if c.f == nil {
		return nil
	}

	err := c.f.Close()
	readerPool.Put(c.r)
	c.f = nil
	return err
}

func readChunkHeader(r io.Reader) (uint64, error) {
	// read the header
	header := make([]byte, len(magicHeader)+1+8)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read header")
	}

	// check the magic number
	if !bytes.Equal(header[:len(magicHeader)], []byte(magicHeader)) {
		return 0, errors.New("invalid magic header")
	}

	// check the version
	if header[len(magicHeader)] != 1 {
		return 0, errors.New("invalid version")
	}

	// read the number of records
	return binary.BigEndian.Uint64(header[len(magicHeader)+1:]), nil
}

// chunkWriter is an io.Writer that writes records to a series of chunk files.
// Each chunk file has a header that contains the number of records in the file.
// The records are written as a 4-byte length followed by the record itself.
// The records are written to a partial chunk file until the target size is reached,
// at which point the partial chunk is promoted to a new chunk file.
// The chunkWriter is not thread-safe and should be used with a lock.
type chunkWriter struct {
	logger      logrus.FieldLogger
	maxSize     uint64
	dir         string
	w           *bufio.Writer
	f           *os.File
	size        uint64
	recordCount uint64
	buf         [8]byte

	reader *chunkReader
}

func newChunkWriter(dir string, reader *chunkReader, logger logrus.FieldLogger, maxSize uint64) (*chunkWriter, error) {
	ch := &chunkWriter{
		dir:     dir,
		reader:  reader,
		logger:  logger,
		maxSize: maxSize,
		w:       bufio.NewWriterSize(nil, chunkWriterBufferSize),
	}

	err := ch.Open()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (w *chunkWriter) Write(buf []byte) (int, error) {
	var created bool

	if w.f == nil {
		err := w.Create()
		if err != nil {
			return 0, err
		}
		created = true
	}

	if w.IsFull() {
		err := w.Promote()
		if err != nil {
			return 0, err
		}

		err = w.Create()
		if err != nil {
			return 0, err
		}
		created = true
	}

	n, err := w.w.Write(buf)
	if err != nil {
		return n, err
	}

	w.size += uint64(n)
	w.recordCount++

	if created {
		return int(w.size), nil
	}

	return n, nil
}

func (w *chunkWriter) Flush() error {
	if w.f == nil {
		return nil
	}

	return w.w.Flush()
}

func (w *chunkWriter) Close() error {
	err := w.w.Flush()
	if err != nil {
		return err
	}

	if w.f != nil {
		err = w.f.Sync()
		if err != nil {
			return errors.Wrap(err, "failed to sync")
		}

		err := w.f.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk")
		}

		w.f = nil
	}

	return nil
}

func (w *chunkWriter) Create() error {
	var err error

	path := filepath.Join(w.dir, fmt.Sprintf(chunkFileFmt, time.Now().UnixMicro()))
	w.f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk file")
	}

	w.w.Reset(w.f)

	// magic
	_, err = w.w.Write([]byte(magicHeader))
	if err != nil {
		return errors.Wrap(err, "failed to write header")
	}
	// version
	err = w.w.WriteByte(1)
	if err != nil {
		return errors.Wrap(err, "failed to write version")
	}
	// number of records
	binary.BigEndian.PutUint64(w.buf[:], uint64(0))
	_, err = w.w.Write(w.buf[:])
	if err != nil {
		return errors.Wrap(err, "failed to write size")
	}
	w.size = uint64(len(magicHeader) + 1 + 8)

	return nil
}

func (w *chunkWriter) Open() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return errors.Wrap(err, "failed to read directory")
	}

	if len(entries) == 0 {
		return nil
	}

	lastChunk := entries[len(entries)-1].Name()

	w.f, err = os.OpenFile(filepath.Join(w.dir, lastChunk), os.O_RDWR, 0o644)
	if err != nil {
		return errors.Wrap(err, "failed to open chunk file")
	}

	w.w.Reset(w.f)

	// get file size
	info, err := w.f.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to stat chunk file")
	}

	// new file, write the header
	if info.Size() == 0 {
		// magic
		_, err = w.w.Write([]byte(magicHeader))
		if err != nil {
			return errors.Wrap(err, "failed to write header")
		}
		// version
		err = w.w.WriteByte(1)
		if err != nil {
			return errors.Wrap(err, "failed to write version")
		}
		// number of records
		binary.BigEndian.PutUint64(w.buf[:], uint64(0))
		_, err = w.w.Write(w.buf[:])
		if err != nil {
			return errors.Wrap(err, "failed to write size")
		}
		w.size = uint64(len(magicHeader) + 1 + 8)

		return nil
	}

	// existing file:
	// either the record count is already written in the header
	// of this is a partial chunk and we need to count the records
	recordCount, err := readChunkHeader(w.f)
	if err != nil {
		return errors.Wrap(err, "failed to read chunk header")
	}

	if recordCount > 0 {
		// the file is a complete chunk
		// close the file and open a new one
		err = w.f.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk file")
		}

		return w.Create()
	}

	w.size = uint64(info.Size())

	r := bufio.NewReader(w.f)

	// count the records by reading the length of each record
	// and skipping it
	var count uint64
	for {
		// read the record length
		n, err := io.ReadFull(r, w.buf[:4])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// a record was not fully written, probably because of a crash.
			w.logger.WithField("action", "queue_log_corruption").
				WithField("path", filepath.Join(w.dir, lastChunk)).
				Error(errors.Wrap(err, "queue ended abruptly, some elements may not have been recovered"))

			// truncate the file to the last complete record
			err = w.f.Truncate(int64(w.size) - int64(n))
			if err != nil {
				return errors.Wrap(err, "failed to truncate chunk file")
			}
			err = w.f.Sync()
			if err != nil {
				return errors.Wrap(err, "failed to sync chunk file")
			}
			w.size -= uint64(n)
			break
		}
		if err != nil {
			return errors.Wrap(err, "failed to read record length")
		}
		length := binary.BigEndian.Uint32(w.buf[:4])
		if length == 0 {
			return errors.New("invalid record length")
		}

		// skip the record
		n, err = r.Discard(int(length))
		if err != nil {
			if errors.Is(err, io.EOF) {
				// a record was not fully written, probably because of a crash.
				w.logger.WithField("action", "queue_log_corruption").
					WithField("path", filepath.Join(w.dir, lastChunk)).
					Error(errors.Wrap(err, "queue ended abruptly, some elements may not have been recovered"))

				// truncate the file to the last complete record
				err = w.f.Truncate(int64(w.size) - 4 - int64(n))
				if err != nil {
					return errors.Wrap(err, "failed to truncate chunk file")
				}
				err = w.f.Sync()
				if err != nil {
					return errors.Wrap(err, "failed to sync chunk file")
				}
				w.size -= 4 + uint64(n)
				break
			}

			return errors.Wrap(err, "failed to skip record")
		}

		count++
	}

	w.recordCount = count

	// place the cursor at the end of the file
	_, err = w.f.Seek(0, 2)
	if err != nil {
		return errors.Wrap(err, "failed to seek to the end of the file")
	}

	return nil
}

func (w *chunkWriter) IsFull() bool {
	return w.f != nil && w.size >= w.maxSize
}

func (w *chunkWriter) Promote() error {
	if w.f == nil {
		return nil
	}

	// flush the buffer
	err := w.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush chunk")
	}

	// update the number of records in the header
	_, err = w.f.Seek(int64(len(magicHeader)+1), 0)
	if err != nil {
		return errors.Wrap(err, "failed to seek to record count")
	}
	err = binary.Write(w.f, binary.BigEndian, w.recordCount)
	if err != nil {
		return errors.Wrap(err, "failed to write record count")
	}

	err = w.reader.PromoteChunk(w.f)
	if err != nil {
		return errors.Wrap(err, "failed to promote chunk")
	}

	w.f = nil
	w.size = 0
	w.recordCount = 0
	w.w.Reset(nil)

	return nil
}

type chunkReader struct {
	m         sync.Mutex
	dir       string
	cursor    int
	chunkList []string
	chunks    map[string]*os.File
}

func newChunkReader(dir string, chunkList []string) *chunkReader {
	return &chunkReader{
		dir:       dir,
		chunks:    make(map[string]*os.File),
		chunkList: chunkList,
	}
}

func (r *chunkReader) ReadChunk() (*chunk, error) {
	r.m.Lock()

	if r.cursor >= len(r.chunkList) {
		r.m.Unlock()
		return nil, nil
	}

	path := r.chunkList[r.cursor]
	f, ok := r.chunks[path]

	r.cursor++

	r.m.Unlock()

	if ok {
		return chunkFromFile(f)
	}

	return openChunk(path)
}

func (r *chunkReader) Close() error {
	r.m.Lock()
	defer r.m.Unlock()

	for _, f := range r.chunks {
		_ = f.Sync()
		_ = f.Close()
	}

	return nil
}

func (r *chunkReader) PromoteChunk(f *os.File) error {
	r.m.Lock()
	// do not keep more than 10 files open
	if len(r.chunks) > 10 {
		r.m.Unlock()

		// sync and close the chunk
		err := f.Sync()
		if err != nil {
			return errors.Wrap(err, "failed to sync chunk")
		}

		err = f.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk")
		}

		// add the file to the list
		r.m.Lock()
		r.chunkList = append(r.chunkList, f.Name())
		r.m.Unlock()

		return nil
	}
	defer r.m.Unlock()

	r.chunks[f.Name()] = f
	r.chunkList = append(r.chunkList, f.Name())

	return nil
}

func (r *chunkReader) RemoveChunk(c *chunk) (bool, error) {
	_ = c.Close()

	r.m.Lock()
	delete(r.chunks, c.path)
	r.m.Unlock()

	err := os.Remove(c.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// already removed
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// compile time check for Queue interface
var _ = Queue(new(DiskQueue))
