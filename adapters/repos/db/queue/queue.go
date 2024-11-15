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

	// m protects the disk operations
	m            sync.RWMutex
	lastPushTime time.Time
	closed       bool
	w            *chunkWriter
	r            *chunkReader
	recordCount  uint64

	rmLock sync.Mutex
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
		staleTimeout: opt.StaleTimeout,
		taskDecoder:  opt.TaskDecoder,
		r:            newChunkReader(opt.Dir),
	}

	// create the directory if it doesn't exist
	err := os.MkdirAll(q.dir, 0o755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	// create or open a partial chunk
	q.w, err = newChunkWriter(opt.Dir, opt.Logger, opt.ChunkSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open or create partial chunk")
	}

	// determine the number of records stored on disk
	q.recordCount, err = q.calculateRecordCount()
	if err != nil {
		return nil, err
	}

	// set the last push time to now
	q.lastPushTime = time.Now()

	return &q, nil
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

	err := q.w.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close chunk writer")
	}

	return nil
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

	_, err = q.w.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	q.recordCount++

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

func (q *DiskQueue) checkIfStale() (*chunk, error) {
	if q.Size() == 0 {
		return nil, nil
	}

	q.m.Lock()

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

	err := q.w.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close partial chunk")
	}

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

	q.m.Lock()
	q.recordCount -= c.count
	q.m.Unlock()
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
	}

	return size + q.w.recordCount, nil
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
}

func newChunkWriter(dir string, logger logrus.FieldLogger, maxSize uint64) (*chunkWriter, error) {
	ch := &chunkWriter{
		dir:     dir,
		logger:  logger,
		maxSize: maxSize,
		w:       bufio.NewWriterSize(nil, 10*1024*1024),
	}

	err := ch.Open()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (p *chunkWriter) Write(buf []byte) (int, error) {
	if p.IsFull() {
		err := p.Promote()
		if err != nil {
			return 0, err
		}

		err = p.Create()
		if err != nil {
			return 0, err
		}
	}

	n, err := p.w.Write(buf)
	if err != nil {
		return n, err
	}

	p.size += uint64(n)
	p.recordCount++

	return n, nil
}

func (p *chunkWriter) Flush() error {
	return p.w.Flush()
}

func (p *chunkWriter) Close() error {
	err := p.w.Flush()
	if err != nil {
		return err
	}

	if p.f != nil {
		err = p.f.Sync()
		if err != nil {
			return errors.Wrap(err, "failed to sync")
		}

		err := p.f.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk")
		}

		p.f = nil
	}

	return nil
}

func (p *chunkWriter) Create() error {
	var err error

	p.f, err = os.OpenFile(filepath.Join(p.dir, partialChunkFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk file")
	}

	p.w.Reset(p.f)

	// magic
	_, err = p.w.Write([]byte(magicHeader))
	if err != nil {
		return errors.Wrap(err, "failed to write header")
	}
	// version
	err = p.w.WriteByte(1)
	if err != nil {
		return errors.Wrap(err, "failed to write version")
	}
	// number of records
	binary.BigEndian.PutUint64(p.buf[:], uint64(0))
	_, err = p.w.Write(p.buf[:])
	if err != nil {
		return errors.Wrap(err, "failed to write size")
	}
	p.size = uint64(len(magicHeader) + 1 + 8)

	return nil
}

func (p *chunkWriter) Open() error {
	var err error

	p.f, err = os.OpenFile(filepath.Join(p.dir, partialChunkFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk file")
	}

	p.w.Reset(p.f)

	// get file size
	info, err := p.f.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to stat chunk file")
	}

	// new file, write the header
	if info.Size() == 0 {
		// magic
		_, err = p.w.Write([]byte(magicHeader))
		if err != nil {
			return errors.Wrap(err, "failed to write header")
		}
		// version
		err = p.w.WriteByte(1)
		if err != nil {
			return errors.Wrap(err, "failed to write version")
		}
		// number of records
		binary.BigEndian.PutUint64(p.buf[:], uint64(0))
		_, err = p.w.Write(p.buf[:])
		if err != nil {
			return errors.Wrap(err, "failed to write size")
		}
		p.size = uint64(len(magicHeader) + 1 + 8)

		return nil
	}

	// existing file, count the number of records
	p.size = uint64(info.Size())

	r := bufio.NewReader(p.f)

	// skip the header
	_, err = r.Discard(len(magicHeader) + 1 + 8)
	if err != nil {
		return errors.Wrap(err, "failed to skip header")
	}

	// count the records by reading the length of each record
	// and skipping it
	var count uint64
	for {
		// read the record length
		_, err := io.ReadFull(r, p.buf[:4])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.Wrap(err, "failed to read record length")
		}
		length := binary.BigEndian.Uint32(p.buf[:4])
		if length == 0 {
			return errors.New("invalid record length")
		}

		// skip the record
		_, err = r.Discard(int(length))
		if err != nil {
			return errors.Wrap(err, "failed to skip record")
		}

		count++
	}

	p.recordCount = count

	// place the cursor at the end of the file
	_, err = p.f.Seek(0, 2)
	if err != nil {
		return errors.Wrap(err, "failed to seek to the end of the file")
	}

	return nil
}

func (p *chunkWriter) IsFull() bool {
	return p.f != nil && p.size >= p.maxSize
}

func (p *chunkWriter) Promote() error {
	if p.f == nil {
		return nil
	}

	fName := p.f.Name()

	// flush the buffer
	err := p.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush chunk")
	}

	// update the number of records in the header
	_, err = p.f.Seek(int64(len(magicHeader)+1), 0)
	if err != nil {
		return errors.Wrap(err, "failed to seek to record count")
	}
	err = binary.Write(p.f, binary.BigEndian, p.recordCount)
	if err != nil {
		return errors.Wrap(err, "failed to write record count")
	}

	// sync and close the chunk
	err = p.f.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync chunk")
	}
	err = p.f.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close chunk")
	}

	p.f = nil
	p.size = 0
	p.recordCount = 0

	// rename the file to remove the .partial suffix
	// and add a timestamp to the filename
	newPath := filepath.Join(p.dir, fmt.Sprintf(chunkFileFmt, time.Now().UnixMilli()))

	err = os.Rename(fName, newPath)
	if err != nil {
		return errors.Wrap(err, "failed to rename chunk file")
	}

	p.logger.WithField("file", newPath).Debug("chunk file created")

	return nil
}

type chunkReader struct {
	dir       string
	chunkList []string
	chunkRead map[string]struct{}
	cursor    int
}

func newChunkReader(dir string) *chunkReader {
	return &chunkReader{
		dir:       dir,
		chunkRead: make(map[string]struct{}),
	}
}

func (r *chunkReader) ReadChunk() (*chunk, error) {
	if r.cursor+1 < len(r.chunkList) {
		r.cursor++

		for r.cursor < len(r.chunkList) {
			if _, ok := r.chunkRead[r.chunkList[r.cursor]]; ok {
				r.cursor++
				continue
			}
			break
		}

		if r.cursor < len(r.chunkList) {
			path := r.chunkList[r.cursor]

			// mark the chunk as read
			r.chunkRead[path] = struct{}{}

			return openChunk(path)
		}
	}

	r.chunkList = r.chunkList[:0]
	r.cursor = 0

	// read the directory
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(r.dir, entry.Name())
		// skip files that have already been read
		if _, ok := r.chunkRead[path]; ok {
			continue
		}

		// check if the entry name matches the regex pattern of a chunk file
		if !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		r.chunkList = append(r.chunkList, path)
	}

	if len(r.chunkList) == 0 {
		return nil, nil
	}

	// make sure the list is sorted
	sort.Strings(r.chunkList)

	r.chunkRead[r.chunkList[r.cursor]] = struct{}{}

	return openChunk(r.chunkList[r.cursor])
}
