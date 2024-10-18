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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// defaultChunkSize is the maximum size of each chunk file. Set to 10MB.
	defaultChunkSize = 10 * 1024 * 1024

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
	DequeueBatch() (batch []Task, done func(), err error)
}

type BeforeScheduleHook interface {
	BeforeSchedule() bool
}

type DiskQueue struct {
	// Logger for the queue. Wrappers of this queue should use this logger.
	Logger       logrus.FieldLogger
	staleTimeout time.Duration
	taskDecoder  TaskDecoder
	scheduler    *Scheduler
	id           string
	dir          string
	lastPushTime atomic.Pointer[time.Time]
	closed       atomic.Bool

	// chunkSize is the maximum size of each chunk file.
	chunkSize uint64

	// m protects the disk operations
	m                sync.RWMutex
	w                bufio.Writer
	f                *os.File
	partialChunkSize uint64
	recordCount      uint64
	readFiles        []string
	cursor           int
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
		opt.StaleTimeout = 5 * time.Second
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
	}

	// create the directory if it doesn't exist
	err := os.MkdirAll(q.dir, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	// determine the number of records stored on disk
	q.recordCount, err = q.calculateRecordCount()
	if err != nil {
		return nil, err
	}

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

func (q *DiskQueue) Push(record []byte) error {
	if q.closed.Load() {
		return errors.New("queue closed")
	}

	if len(record) == 0 {
		return errors.New("empty record")
	}

	now := time.Now()
	q.lastPushTime.Store(&now)

	q.m.Lock()
	defer q.m.Unlock()

	err := q.ensureChunk()
	if err != nil {
		return err
	}

	// length of the record in 4 bytes
	err = binary.Write(&q.w, binary.BigEndian, uint32(len(record)))
	if err != nil {
		return errors.Wrap(err, "failed to write record length")
	}

	// write the record
	n, err := q.w.Write(record)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	q.partialChunkSize += 4 + uint64(n)
	q.recordCount++

	return nil
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
			err = binary.Write(&q.w, binary.BigEndian, uint64(0))
			if err != nil {
				return errors.Wrap(err, "failed to write size")
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

func (q *DiskQueue) DequeueBatch() (batch []Task, done func(), err error) {
	f, path, err := q.readChunk()
	if err != nil {
		return nil, nil, err
	}

	// if there are no more chunks to read,
	// check if the partial chunk is stale (e.g no tasks were pushed for a while)
	if f == nil && q.Size() > 0 {
		f, path, err = q.checkIfStale()
		if err != nil || f == nil {
			return nil, nil, err
		}
	}

	if f == nil {
		return nil, nil, nil
	}

	defer f.Close()

	// decode all tasks from the chunk
	// and partition them by worker
	r := bufio.NewReader(f)

	var tasks []Task

	buf := make([]byte, 4)
	for {
		buf = buf[:4]

		// read the record length
		_, err := io.ReadFull(r, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to read record length")
		}
		length := binary.BigEndian.Uint32(buf)
		if length == 0 {
			return nil, nil, errors.New("invalid record length")
		}

		// read the record
		if cap(buf) < int(length) {
			buf = make([]byte, length)
		} else {
			buf = buf[:length]
		}
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to read record")
		}

		// decode the task
		t, err := q.taskDecoder.DecodeTask(buf)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to decode task")
		}

		tasks = append(tasks, t)
	}

	err = f.Close()
	if err != nil {
		q.Logger.WithField("file", path).WithError(err).Warn("failed to close chunk file")
	}

	if len(tasks) == 0 {
		// empty chunk, remove it
		q.removeChunk(path)
		return nil, nil, nil
	}

	doneFn := func() {
		q.removeChunk(path)
	}

	return tasks, doneFn, nil
}

func (q *DiskQueue) readChunk() (*os.File, string, error) {
	if q.cursor+1 < len(q.readFiles) {
		q.cursor++

		f, err := os.Open(q.readFiles[q.cursor])
		if err != nil {
			return nil, "", err
		}

		return f, q.readFiles[q.cursor], nil
	}

	q.readFiles = q.readFiles[:0]
	q.cursor = 0

	// read the directory
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, "", err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// check if the entry name matches the regex pattern of a chunk file
		if !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		q.readFiles = append(q.readFiles, filepath.Join(q.dir, entry.Name()))
	}

	if len(q.readFiles) == 0 {
		return nil, "", nil
	}

	f, err := os.Open(q.readFiles[q.cursor])
	if err != nil {
		return nil, "", err
	}

	return f, q.readFiles[q.cursor], nil
}

func (q *DiskQueue) checkIfStale() (*os.File, string, error) {
	lastPushed := q.lastPushTime.Load()
	if lastPushed == nil {
		return nil, "", nil
	}

	if time.Since(*lastPushed) < q.staleTimeout {
		return nil, "", nil
	}

	q.Logger.Debug("partial chunk is stale, scheduling")

	err := q.promoteChunk()
	if err != nil {
		return nil, "", err
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

func (q *DiskQueue) removeChunk(path string) {
	q.m.Lock()
	defer q.m.Unlock()

	count, err := q.readChunkRecordCount(path)
	if err != nil {
		if os.IsNotExist(err) {
			// already removed
			return
		}

		q.Logger.WithError(err).WithField("file", path).Warn("failed to read chunk record count")
		// TODO: recalculate the record count?
	}

	err = os.Remove(path)
	if err != nil {
		q.Logger.WithError(err).WithField("file", path).Error("failed to remove chunk")
		return
	}

	q.recordCount -= count

	q.Logger.WithField("file", path).Debug("chunk removed")
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

			size += count
			continue
		}
	}

	return size, nil
}

func (q *DiskQueue) readChunkRecordCount(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, errors.Wrap(err, "failed to open chunk file")
	}
	defer f.Close()

	r := bufio.NewReader(f)

	// read the header
	header := make([]byte, len(magicHeader)+1+8)
	_, err = io.ReadFull(r, header)
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

func (q *DiskQueue) countPartialChunkRecords(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, errors.Wrap(err, "failed to open chunk file")
	}
	defer f.Close()

	r := bufio.NewReader(f)

	// read the header
	header := make([]byte, len(magicHeader)+1)
	_, err = io.ReadFull(r, header)
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

	// skip the number of records
	_, err = r.Discard(8)
	if err != nil {
		return 0, errors.Wrap(err, "failed to skip header")
	}

	// count the records by reading the length of each record
	// and skipping it
	var count uint64
	var buf [4]byte
	for {
		// read the record length
		_, err := io.ReadFull(r, buf[:])
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
		_, err = r.Discard(int(length))
		if err != nil {
			return 0, errors.Wrap(err, "failed to skip record")
		}

		count++
	}

	return count, nil
}
