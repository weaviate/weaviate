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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// defaultChunkSize is the maximum size of each chunk file. Set to 100MB.
	defaultChunkSize = 100 * 1024 * 1024

	// name of the file that stores records before they reach the target size.
	partialChunkFile = "chunk.bin.partial"
	// chunkFileFmt is the format string for the promoted chunk files,
	// i.e. files that have reached the target size, or those that are
	// stale and need to be promoted.
	chunkFileFmt = "chunk-%d.bin"

	magicHeader = "WV8Q"
)

// regex pattern for the chunk files
var chunkFilePattern = regexp.MustCompile(`chunk-\d+\.bin`)

type Queue interface {
	ID() string
	Size() int64
	DequeueBatch() (batch *Batch, err error)
}

type BeforeScheduleHook interface {
	BeforeSchedule() bool
}

var _ Queue = &DiskQueue{}

type DiskQueue struct {
	// Logger for the queue. Wrappers of this queue should use this logger.
	Logger       logrus.FieldLogger
	staleTimeout time.Duration
	taskDecoder  TaskDecoder
	scheduler    *Scheduler
	id           string
	dir          string
	lastPushTime atomic.Pointer[time.Time]
	chunkSize    uint64

	// m protects the disk operations
	m                       sync.RWMutex
	closed                  bool
	w                       *bufio.Writer
	f                       *os.File
	partialChunkSize        uint64
	partialChunkRecordCount uint64
	recordCount             uint64
	chunkList               []string
	chunkRead               map[string]struct{}
	cursor                  int
}

type DiskQueueOptions struct {
	// Required
	ID          string
	Scheduler   *Scheduler
	Dir         string
	TaskDecoder TaskDecoder

	// Optional
	Logger       logrus.FieldLogger
	StaleTimeout time.Duration
	ChunkSize    uint64
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
	opt.Logger = opt.Logger.WithField("queue_id", opt.ID)
	if opt.StaleTimeout <= 0 {
		d, err := time.ParseDuration(os.Getenv("ASYNC_INDEXING_STALE_TIMEOUT"))
		if err == nil {
			opt.StaleTimeout = d
		} else {
			opt.StaleTimeout = 1 * time.Second
		}
	}
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = defaultChunkSize
	}

	q := DiskQueue{
		id:           opt.ID,
		scheduler:    opt.Scheduler,
		dir:          opt.Dir,
		Logger:       opt.Logger,
		chunkSize:    opt.ChunkSize,
		staleTimeout: opt.StaleTimeout,
		taskDecoder:  opt.TaskDecoder,
		w:            bufio.NewWriterSize(nil, 64*1024),
		chunkRead:    make(map[string]struct{}),
	}

	// create the directory if it doesn't exist
	err := os.MkdirAll(q.dir, 0o755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	// determine the number of records stored on disk
	q.recordCount, err = q.calculateRecordCount()
	if err != nil {
		return nil, err
	}

	// ensure the partial chunk is created or loaded
	err = q.ensureChunk()
	if err != nil {
		return nil, err
	}

	// set the last push time to now
	now := time.Now()
	q.lastPushTime.Store(&now)

	return &q, nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *DiskQueue) Close() error {
	if q == nil {
		return nil
	}

	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return errors.New("queue already closed")
	}
	q.closed = true

	q.scheduler.UnregisterQueue(q.id)

	err := q.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush")
	}

	if q.f != nil {
		err = q.f.Sync()
		if err != nil {
			return errors.Wrap(err, "failed to sync")
		}

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

func (q *DiskQueue) Push(record []byte) error {
	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return errors.New("queue closed")
	}

	if len(record) == 0 {
		return errors.New("empty record")
	}

	now := time.Now()
	q.lastPushTime.Store(&now)

	err := q.ensureChunk()
	if err != nil {
		return err
	}

	// length of the record in 4 bytes
	err = binary.Write(q.w, binary.BigEndian, uint32(len(record)))
	if err != nil {
		return errors.Wrap(err, "failed to write record length")
	}

	// write the record
	n, err := q.w.Write(record)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	q.partialChunkSize += 4 + uint64(n)
	q.partialChunkRecordCount++
	q.recordCount++

	return nil
}

func (q *DiskQueue) Scheduler() *Scheduler {
	return q.scheduler
}

func (q *DiskQueue) ensureChunk() error {
	if q.f != nil && q.partialChunkSize >= q.chunkSize {
		err := q.promoteChunkNoLock()
		if err != nil {
			return err
		}
	}

	if q.f == nil {
		// create or open partial chunk
		path := filepath.Join(q.dir, partialChunkFile)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
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

		q.partialChunkSize = uint64(info.Size())

		if info.Size() == 0 {
			// magic
			_, err = q.w.Write([]byte(magicHeader))
			if err != nil {
				return errors.Wrap(err, "failed to write header")
			}
			// version
			err = q.w.WriteByte(1)
			if err != nil {
				return errors.Wrap(err, "failed to write version")
			}
			// number of records
			err = binary.Write(q.w, binary.BigEndian, uint64(0))
			if err != nil {
				return errors.Wrap(err, "failed to write size")
			}
		} else {
			// place the cursor at the end of the file
			_, err = f.Seek(0, 2)
			if err != nil {
				return errors.Wrap(err, "failed to seek to the end of the file")
			}
		}
	}

	return nil
}

func (q *DiskQueue) promoteChunkNoLock() error {
	if q.f == nil {
		return nil
	}

	fName := q.f.Name()

	// flush the buffer
	err := q.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush chunk")
	}

	// update the number of records in the header
	_, err = q.f.Seek(int64(len(magicHeader)+1), 0)
	if err != nil {
		return errors.Wrap(err, "failed to seek to record count")
	}
	err = binary.Write(q.f, binary.BigEndian, q.partialChunkRecordCount)
	if err != nil {
		return errors.Wrap(err, "failed to write record count")
	}

	// sync and close the chunk
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
	q.partialChunkRecordCount = 0

	// rename the file to remove the .partial suffix
	// and add a timestamp to the filename
	newPath := filepath.Join(q.dir, fmt.Sprintf(chunkFileFmt, time.Now().UnixMilli()))
	err = os.Rename(fName, newPath)
	if err != nil {
		return errors.Wrap(err, "failed to rename chunk file")
	}

	q.Logger.WithField("file", newPath).Debug("chunk file created")

	return nil
}

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

func (q *DiskQueue) DequeueBatch() (batch *Batch, err error) {
	c, err := q.readChunk()
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
			return nil, errors.Wrap(err, "failed to read record:")
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
	}

	return &Batch{
		Tasks:  tasks,
		onDone: doneFn,
	}, nil
}

func (q *DiskQueue) readChunk() (*chunk, error) {
	if q.cursor+1 < len(q.chunkList) {
		q.cursor++

		for q.cursor < len(q.chunkList) {
			if _, ok := q.chunkRead[q.chunkList[q.cursor]]; ok {
				q.cursor++
				continue
			}
			break
		}

		if q.cursor < len(q.chunkList) {
			path := q.chunkList[q.cursor]

			// mark the chunk as read
			q.chunkRead[path] = struct{}{}

			return openChunk(path)
		}
	}

	q.chunkList = q.chunkList[:0]
	q.cursor = 0

	// read the directory
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(q.dir, entry.Name())
		// skip files that have already been read
		if _, ok := q.chunkRead[path]; ok {
			continue
		}

		// check if the entry name matches the regex pattern of a chunk file
		if !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		q.chunkList = append(q.chunkList, path)
	}

	if len(q.chunkList) == 0 {
		return nil, nil
	}

	// make sure the list is sorted
	sort.Strings(q.chunkList)

	q.chunkRead[q.chunkList[q.cursor]] = struct{}{}

	return openChunk(q.chunkList[q.cursor])
}

func (q *DiskQueue) checkIfStale() (*chunk, error) {
	if q.Size() == 0 {
		return nil, nil
	}

	q.m.Lock()
	defer q.m.Unlock()

	if q.partialChunkRecordCount == 0 {
		return nil, nil
	}

	lastPushed := q.lastPushTime.Load()
	if lastPushed == nil {
		return nil, nil
	}

	if time.Since(*lastPushed) < q.staleTimeout || q.partialChunkRecordCount == 0 {
		return nil, nil
	}

	q.Logger.Debug("partial chunk is stale, scheduling")

	err := q.promoteChunkNoLock()
	if err != nil {
		return nil, err
	}

	return q.readChunk()
}

func (q *DiskQueue) Size() int64 {
	q.m.RLock()
	defer q.m.RUnlock()

	return int64(q.recordCount)
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

func (q *DiskQueue) removeChunk(c *chunk) {
	q.m.Lock()
	defer q.m.Unlock()

	_ = c.Close()

	err := os.Remove(c.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// already removed
			return
		}

		q.Logger.WithError(err).WithField("file", c.path).Error("failed to remove chunk")
		return
	}

	q.recordCount -= c.count
}

// calculateRecordCount is a slow method that determines the number of records
// stored on disk and in the partial chunk, by reading the header of all the files in the directory.
// It is used when the queue is first initialized.
func (q *DiskQueue) calculateRecordCount() (uint64, error) {
	q.m.Lock()
	defer q.m.Unlock()

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read directory")
	}

	var size uint64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// read the header of the chunk file
		if chunkFilePattern.Match([]byte(entry.Name())) {
			count, err := q.readChunkRecordCount(filepath.Join(q.dir, entry.Name()))
			if err != nil {
				return 0, err
			}

			size += count
			continue
		}

		// manually count the records in the partial chunk
		if entry.Name() == partialChunkFile {
			count, err := q.countPartialChunkRecords(filepath.Join(q.dir, entry.Name()))
			if err != nil {
				return 0, err
			}

			q.partialChunkRecordCount = count
			size += count
			continue
		}
	}

	return size, nil
}

func (q *DiskQueue) readChunkRecordCount(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return readChunkHeader(f)
}

func (q *DiskQueue) countPartialChunkRecords(path string) (uint64, error) {
	c, err := openChunk(path)
	if err != nil {
		return 0, errors.Wrap(err, "failed to open chunk file")
	}
	defer c.Close()

	// count the records by reading the length of each record
	// and skipping it
	var count uint64
	var buf [4]byte
	for {
		// read the record length
		_, err := io.ReadFull(c.r, buf[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, errors.Wrap(err, "failed to read record length")
		}
		length := binary.BigEndian.Uint32(buf[:])
		if length == 0 {
			return 0, errors.New("invalid record length")
		}

		// skip the record
		_, err = c.r.Discard(int(length))
		if err != nil {
			return 0, errors.Wrap(err, "failed to skip record")
		}

		count++
	}

	return count, nil
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

	c.r = readerPool.Get().(*bufio.Reader)
	c.r.Reset(c.f)

	// check the header
	c.count, err = readChunkHeader(c.r)
	if err != nil {
		return nil, err
	}

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
