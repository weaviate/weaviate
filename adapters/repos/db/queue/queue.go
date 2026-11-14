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

// Queue package implements a queue system for background operations using disk storage.
// It provides a DiskQueue that stores tasks in chunk files on disk, allowing for
// efficient handling of large volumes of tasks without consuming excessive memory.
// The Scheduler manages multiple queues and schedules task processing to a fixed number of workers.
package queue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/diskio"
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

	// chunkHeaderSize is the size of a chunk file header:
	// magic + version + record count.
	chunkHeaderSize = len(magicHeader) + 1 + 8

	// minEncodedRecordSize is the smallest possible record footprint in a
	// chunk payload: a 4-byte length prefix plus at least 1 byte of record data.
	minEncodedRecordSize = 4 + 1
)

// regex pattern for the chunk files.
// It is anchored so that derived files (e.g. .processed tombstones or
// .corrupt quarantined chunks) never match.
var chunkFilePattern = regexp.MustCompile(`^chunk-\d+\.bin$`)

// errBadMagic and errUnknownVersion let init-time recovery distinguish a
// corrupt header from a well-formed header written by a newer version.
var (
	errBadMagic       = errors.New("invalid magic header")
	errUnknownVersion = errors.New("invalid version")
	errEmptyChunk     = errors.New("empty chunk file")
	errChunkProcessed = errors.New("chunk already processed")
)

// regex pattern for processed chunk tombstone files
var processedChunkFilePattern = regexp.MustCompile(`^chunk-\d+\.bin\.processed$`)

// A Queue represents anything that can be scheduled by the Scheduler.
// It must return its ID, size, and be able to dequeue a batch of tasks.
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
	onBatchProcessed func() error
	onChunkRemoved   func()
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

	// maintenanceMode prevents processed chunk files from being deleted from disk;
	// instead a tombstone (.bin.processed) is created alongside them.
	// This is used during backup to keep chunk files available for copying.
	maintenanceMode atomic.Bool

	// retained keeps processed chunks the same way for a replacement index,
	// from the chunk numbered retainedFrom on, see Retain. Guarded by rmLock.
	retained     bool
	retainedFrom int64
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
	OnBatchProcessed func() error
	// OnChunkRemoved runs once a chunk left the queue and its records are
	// no longer counted in Size.
	OnChunkRemoved func()
	Metrics        *Metrics
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
		onChunkRemoved:   opt.OnChunkRemoved,
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
	// the writer may have cut a torn record off the partial chunk, or started
	// a new chunk
	q.recordCount += q.w.recordCount
	q.diskUsage += int64(q.w.size)

	// set the last push time to now
	q.lastPushTime = time.Now()

	return nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *DiskQueue) Close(ctx context.Context) error {
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

	q.scheduler.UnregisterQueue(ctx, q.id)

	q.m.Lock()
	defer q.m.Unlock()

	var errs []error

	if q.w != nil {
		err := q.w.Close()
		if err != nil {
			errs = append(errs, errors.Wrap(err, "failed to close chunk writer"))
		}
	}

	if q.r != nil {
		err := q.r.Close()
		if err != nil {
			errs = append(errs, errors.Wrap(err, "failed to close chunk reader"))
		}
	}

	return stderrors.Join(errs...)
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
	q.metrics.Size(q.recordCount)
	q.metrics.DiskUsage(q.diskUsage)

	return nil
}

func (q *DiskQueue) Scheduler() *Scheduler {
	return q.scheduler
}

func (q *DiskQueue) Flush() error {
	q.m.Lock()
	defer q.m.Unlock()

	if q.w == nil {
		return nil
	}

	return q.w.Flush()
}

func (q *DiskQueue) DequeueBatch() (batch *Batch, err error) {
	c, err := q.readChunk()
	if err != nil {
		return nil, err
	}

	// if there are no more chunks to read,
	// check if the partial chunk is stale (e.g no tasks were pushed for a while)
	if c == nil {
		c, err = q.checkIfStale()
		if c == nil || err != nil {
			return nil, err
		}
	}

	defer c.Close()

	tasks, corruptChunkErr, err := q.decodeChunk(c)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		// nothing to process
		return nil, q.finishChunk(c.path, corruptChunkErr)
	}

	doneFn := func() {
		// the chunk is the only copy of its records until the index flushed
		// them: if the flush fails, keep it so the next read replays it
		if q.onBatchProcessed != nil {
			err := q.onBatchProcessed()
			if err != nil {
				q.Logger.WithField("file", c.path).Errorf("failed to flush the index, keeping the processed chunk for replay: %v", err)
				// the replayed tasks may succeed again, so only the delay
				// keeps a persistent failure from replaying in a loop
				q.scheduler.retryLater(q.id)
				return
			}
		}

		// a corrupt chunk is only quarantined once its salvaged records are
		// processed, so a canceled batch can salvage them again
		err := q.finishChunk(c.path, corruptChunkErr)
		if err != nil {
			// keep it without processing its records again: the next read
			// retries
			q.r.markProcessed(c.path, corruptChunkErr)
			q.Logger.WithField("file", c.path).Errorf("failed to remove processed chunk, will retry: %v", err)
		}
	}

	return &Batch{
		Tasks:  tasks,
		OnDone: doneFn,
	}, nil
}

// decodeChunk reads the tasks of a chunk. A chunk torn by a crash (e.g. power
// loss before its tail reached disk) ends mid-record: the records before the
// corruption are still returned, along with the corruption, so that the chunk
// is quarantined once they are processed instead of never being removed.
func (q *DiskQueue) decodeChunk(c *chunk) (tasks []Task, corrupt error, err error) {
	// The record count comes from disk and may be corrupt. It is only used as
	// a capacity hint: cap it by the number of records that could possibly
	// fit in the chunk payload.
	payloadSize := c.size - uint64(chunkHeaderSize)
	count := c.count
	if maxRecords := payloadSize / minEncodedRecordSize; count > maxRecords {
		q.Logger.WithField("file", c.path).
			Warnf("chunk header claims %d records, but at most %d fit in %d bytes of payload; the header is likely corrupt", c.count, maxRecords, payloadSize)
		count = maxRecords
	}

	tasks = make([]Task, 0, count)
	var undecodable int
	var decodeErr error

	buf := make([]byte, 4)
	for {
		record, damaged, err := c.nextRecord(buf)
		if err != nil {
			return nil, nil, err
		}
		if record == nil {
			corrupt = damaged
			break
		}
		buf = record

		// decode the task. The framing is intact, so a record that cannot
		// be decoded is skipped and the following ones are still read.
		t, err := q.decodeTask(record)
		if err != nil {
			undecodable++
			decodeErr = err
			continue
		}

		tasks = append(tasks, t)
	}

	// a chunk cut at a record boundary reads cleanly up to the cut
	if read := uint64(len(tasks) + undecodable); corrupt == nil && read < c.count {
		corrupt = errors.Errorf("chunk holds %d records, its header claims %d", read, c.count)
	}

	// an intact chunk where no record decodes points to the decoder rather
	// than to the data: keep it for a fixed binary instead of quarantining it.
	// A single record tells nothing about the decoder.
	if len(tasks) == 0 && undecodable > 1 && corrupt == nil {
		return nil, nil, errors.Wrapf(decodeErr, "none of the %d records could be decoded", undecodable)
	}

	if undecodable > 0 {
		decodeErr = errors.Wrapf(decodeErr, "%d records could not be decoded", undecodable)
		corrupt = stderrors.Join(corrupt, decodeErr)
	}

	return tasks, corrupt, nil
}

// Pending calls fn with the tasks waiting on disk, oldest first, until it returns
// false. The tasks stay in the queue. It has to run before the queue is scheduled. A
// damaged record is passed over, the workers deal with it when they get there.
func (q *DiskQueue) Pending(fn func(Task) bool) error {
	paths := make([]string, 0, len(q.r.chunkList)+1)
	for _, ref := range q.r.chunkList {
		paths = append(paths, ref.path)
	}
	// the partial chunk holds the newest records, and its header doesn't count them
	if q.w.recordCount > 0 {
		paths = append(paths, q.w.f.Name())
	}

	for _, path := range paths {
		more, err := q.pendingInChunk(path, fn)
		if !more || err != nil {
			return err
		}
	}
	return nil
}

func (q *DiskQueue) pendingInChunk(path string, fn func(Task) bool) (more bool, err error) {
	c, err := openChunk(path)
	if err != nil {
		return false, err
	}
	defer c.Close()

	buf := make([]byte, 4)
	for {
		record, _, err := c.nextRecord(buf)
		if err != nil {
			return false, err
		}
		if record == nil {
			return true, nil
		}
		buf = record

		t, err := q.decodeTask(record)
		if err != nil {
			continue
		}
		if !fn(t) {
			return false, nil
		}
	}
}

// decodeTask decodes a record. A decoder may panic on a malformed record:
// that is a decoding error too, or the chunk would fail on every read.
func (q *DiskQueue) decodeTask(data []byte) (t Task, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("panic while decoding task: %v", r)
		}
	}()

	return q.taskDecoder.DecodeTask(data)
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

	return q.readChunk()
}

// readChunk returns the oldest chunk not processed yet. A chunk whose file is
// gone or whose header became unreadable is taken out of the queue, and the
// next one is read. Any other error may be temporary: the chunk stays first.
func (q *DiskQueue) readChunk() (*chunk, error) {
	for {
		c, err := q.r.ReadChunk()
		var readErr *chunkReadError
		if !stderrors.As(err, &readErr) {
			return c, err
		}

		ref := readErr.ref
		switch {
		case stderrors.Is(err, errChunkProcessed):
			err := q.finishChunk(ref.path, ref.corrupt)
			if err != nil {
				return nil, err
			}
		case stderrors.Is(err, fs.ErrNotExist):
			q.dropMissingChunk(ref)
		case stderrors.Is(err, errEmptyChunk):
			err := q.removeChunk(ref.path)
			if err != nil {
				return nil, err
			}
			q.Logger.WithField("file", ref.path).Errorf("chunk file is empty, its %d records are lost", ref.count)
		case isCorruptHeader(err):
			err := q.quarantineChunk(ref.path, err)
			if err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
}

// isCorruptHeader reports whether a chunk's header became unreadable after
// startup. Unlike at startup, an unknown version is corruption here: this
// binary already read the header or wrote it.
func isCorruptHeader(err error) bool {
	return stderrors.Is(err, errBadMagic) ||
		stderrors.Is(err, errUnknownVersion) ||
		stderrors.Is(err, io.EOF) ||
		stderrors.Is(err, io.ErrUnexpectedEOF)
}

// dropMissingChunk takes a chunk whose file is gone out of the queue.
func (q *DiskQueue) dropMissingChunk(ref chunkRef) {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	q.forgetChunk(ref.path)
	q.Logger.WithField("file", ref.path).Errorf("chunk file is missing, its %d records are lost", ref.count)
}

func (q *DiskQueue) Size() int64 {
	if q == nil {
		return 0
	}

	q.m.RLock()
	defer q.m.RUnlock()

	return int64(q.recordCount)
}

// Pause the dequeuing of tasks. If nowait is true, it returns immediately
// without waiting for the currently running tasks to finish.
// This does not prevent pushing new tasks to the queue.
func (q *DiskQueue) Pause(ctx context.Context, nowait ...bool) error {
	q.scheduler.PauseQueue(q.id)
	if len(nowait) == 0 || !nowait[0] {
		return q.scheduler.Wait(ctx, q.id)
	}
	return nil
}

// Resume the dequeuing of tasks.
func (q *DiskQueue) Resume() {
	q.scheduler.ResumeQueue(q.id)
}

// Wait blocks until all currently running tasks are finished.
func (q *DiskQueue) Wait(ctx context.Context) error {
	return q.scheduler.Wait(ctx, q.id)
}

// PrepareForBackup pauses the queue and flushes all tasks to disk to prepare
// for backup. It also promotes the current partial chunk into a sealed file
// and enables maintenance mode, which prevents processed chunk files from
// being deleted until the backup is complete.
//
// Promoting the partial chunk ensures that no open writer can modify files in
// the queue directory while they are being uploaded. Without this, the resumed
// queue writer could modify chunk files mid-upload, causing checksum mismatches
// (e.g. S3 BadDigest errors).
func (q *DiskQueue) PrepareForBackup(ctx context.Context) error {
	err := q.Pause(ctx)
	if err != nil {
		q.Resume()
		return err
	}
	defer q.Resume()

	err = q.Flush()
	if err != nil {
		return err
	}

	// Seal the current partial chunk so the writer starts a fresh file on
	// Resume. This guarantees all existing chunk files are immutable during
	// the upload.
	q.m.Lock()
	if q.w != nil {
		err = q.w.Promote()
	}
	q.m.Unlock()
	if err != nil {
		return errors.Wrap(err, "promote partial chunk for backup")
	}

	q.EnableMaintenanceMode()

	return nil
}

// ForceSwitch forces the queue to switch to a new chunk file.
// It also returns the content of the directory before the switch.
// Important: the queue must be paused before calling this method.
func (q *DiskQueue) ForceSwitch(ctx context.Context, basePath string) ([]string, error) {
	q.m.Lock()
	defer q.m.Unlock()

	// if the writer is nil, the queue is not initialized
	if q.w == nil {
		return nil, nil
	}

	// promote the current partial chunk
	err := q.w.Promote()
	if err != nil {
		return nil, errors.Wrap(err, "failed to promote chunk")
	}

	return q.listFilesNoLock(ctx, basePath)
}

func (q *DiskQueue) Drop(ctx context.Context) error {
	if q == nil {
		return nil
	}

	err := q.Close(ctx)
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

// EnableMaintenanceMode prevents processed chunk files from being deleted.
// Instead, a tombstone file (.bin.processed) is created alongside each processed chunk.
// This allows the backup system to copy chunk files without them disappearing mid-copy.
// Call DisableMaintenanceMode when the backup is complete.
func (q *DiskQueue) EnableMaintenanceMode() {
	q.maintenanceMode.Store(true)
}

// DisableMaintenanceMode re-enables normal chunk deletion and cleans up all
// tombstone files and their corresponding chunk files left from the freeze
// period, unless a retention still keeps them.
func (q *DiskQueue) DisableMaintenanceMode() error {
	tombstones, err := q.endMaintenanceMode()
	if err != nil {
		return err
	}
	// without rmLock, as in ReleaseRetention
	q.logRemoveProcessedChunks(tombstones)
	return nil
}

// endMaintenanceMode turns maintenance mode off and lists the tombstones of
// the chunks nothing keeps anymore.
func (q *DiskQueue) endMaintenanceMode() ([]string, error) {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	q.maintenanceMode.Store(false)
	if q.retained {
		return q.processedChunks(q.retainedFrom)
	}
	return q.processedChunks(math.MaxInt64)
}

// keepsProcessed reports whether a processed chunk is kept rather than
// deleted. The caller holds rmLock.
func (q *DiskQueue) keepsProcessed() bool {
	return q.maintenanceMode.Load() || q.retained
}

// Retain keeps everything queued from now on or not processed yet, processed
// or not, until ReleaseRetention: a replacement index replays it, see Rewind,
// while the current consumer carries on. Processed chunks are kept as a backup
// keeps them, behind a .processed tombstone, so a restart deletes them unless
// Rewind runs before Init. Chunks a backup kept from before Retain go when the
// backup ends. A queue is retained once at a time.
func (q *DiskQueue) Retain() error {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if q.r == nil || q.w == nil {
		return errors.New("queue not initialized")
	}
	if q.retained {
		return errors.New("queue already retained")
	}

	// under the writer's lock, so no chunk is created in between
	q.m.Lock()
	defer q.m.Unlock()
	first := q.r.firstUnprocessed()
	if first == "" && q.w.f != nil {
		first = q.w.f.Name()
	}
	if first == "" {
		first = chunkPath(q.dir, q.w.nextSeq())
	}
	from, ok := chunkSeq(first)
	if !ok {
		return fmt.Errorf("unexpected chunk name %q", first)
	}
	q.retained = true
	q.retainedFrom = from
	return nil
}

// Rewind serves every kept chunk again, in order, ahead of what is still
// queued. A running queue must be retained, not held by a backup, and paused
// with no batch left: a batch finishing after the rewind would tombstone its
// chunk out of the replay. On a queue not initialized yet it only
// makes the kept chunks ordinary ones for Init, which would delete them
// otherwise; that is how a restart resumes one. Doing it twice replays at most
// what a backup kept in between. The retention stays until ReleaseRetention,
// which must not overlap it: a release deleting would take what it re-queues,
// so the retention's one owner calls them in sequence.
func (q *DiskQueue) Rewind() error {
	// not q.m around the disk work: Push and the scheduler's Size need it
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	live := q.r != nil
	if live && !q.retained {
		return errors.New("queue not retained")
	}
	if live && !q.scheduler.IsQueueIdle(q.id) {
		return errors.New("queue not paused, or a batch is still running")
	}
	// a backup may be copying the chunks this would move or re-queue
	if live && q.maintenanceMode.Load() {
		return errors.New("queue in maintenance mode")
	}

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		if stderrors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return errors.Wrap(err, "failed to read directory during rewind")
	}

	var refs []chunkRef
	var errs []error
	for _, entry := range entries {
		if !processedChunkFilePattern.MatchString(entry.Name()) {
			continue
		}
		tombstonePath := filepath.Join(q.dir, entry.Name())
		path := strings.TrimSuffix(tombstonePath, ".processed")
		// a hard link from quarantineKeptChunk would make the rename of the
		// chunk's next quarantine a no-op, leaving it to break a later Init
		_, err = os.Stat(path)
		if err == nil {
			err = removeFile(path + ".corrupt")
			if err != nil && !stderrors.Is(err, fs.ErrNotExist) {
				errs = append(errs, errors.Wrap(err, "failed to remove quarantined copy"))
				continue
			}
		}
		var ref chunkRef
		var ok bool
		if live {
			ref, ok, err = q.keptChunkRef(path)
			if err != nil {
				errs = append(errs, err)
				continue
			}
		}
		err = removeFile(tombstonePath)
		if err != nil && !stderrors.Is(err, fs.ErrNotExist) {
			errs = append(errs, errors.Wrap(err, "failed to remove tombstone"))
			continue
		}
		if ok {
			refs = append(refs, ref)
		}
	}
	// a tombstone back after a crash would delete a chunk not replayed yet
	err = diskio.Fsync(q.dir)
	if err != nil {
		errs = append(errs, errors.Wrap(err, "failed to sync directory"))
	}

	if live {
		added := q.r.rewind(refs)
		q.m.Lock()
		for _, ref := range added {
			q.recordCount += ref.count
			q.diskUsage += int64(ref.size)
		}
		q.metrics.DiskUsage(q.diskUsage)
		q.metrics.Size(q.recordCount)
		q.m.Unlock()
	}
	return stderrors.Join(errs...)
}

// keptChunkRef reads what a kept chunk counts for. ok is false when it holds
// nothing to serve: its file is gone, or its header no longer reads, in which
// case it is quarantined.
func (q *DiskQueue) keptChunkRef(path string) (ref chunkRef, ok bool, err error) {
	info, err := os.Stat(path)
	if stderrors.Is(err, fs.ErrNotExist) {
		return chunkRef{}, false, nil
	}
	if err != nil {
		return chunkRef{}, false, errors.Wrap(err, "failed to stat kept chunk")
	}
	count, err := q.readChunkRecordCount(path)
	if isCorruptHeader(err) {
		q.Logger.WithField("file", path).Errorf("quarantining kept chunk with unreadable header, its tasks are lost: %v", err)
		err = os.Rename(path, path+".corrupt")
		if err != nil {
			return chunkRef{}, false, errors.Wrap(err, "failed to quarantine kept chunk")
		}
		return chunkRef{}, false, nil
	}
	if err != nil {
		return chunkRef{}, false, errors.Wrap(err, "failed to read kept chunk")
	}
	return chunkRef{path: path, count: count, size: uint64(info.Size())}, true, nil
}

// ReleaseRetention stops keeping chunks and deletes the processed ones,
// unless a backup still keeps them. Releasing a queue not retained is a
// no-op. rmLock is not held during the deletes: a worker finishing a batch of
// this queue needs it, and workers are shared with every other queue.
func (q *DiskQueue) ReleaseRetention() error {
	for {
		tombstones, done, err := q.releaseIfNothingKept()
		if done || err != nil {
			return err
		}
		err = removeProcessedChunks(tombstones)
		if err != nil {
			return errors.Wrap(err, "failed to remove processed chunks")
		}
	}
}

// releaseIfNothingKept ends the retention if no processed chunk is left to
// delete, or a backup takes them over. Otherwise it returns their tombstones
// and stays retained, so a failed release can be retried.
func (q *DiskQueue) releaseIfNothingKept() (tombstones []string, done bool, err error) {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if !q.retained {
		return nil, true, nil
	}
	if !q.maintenanceMode.Load() {
		tombstones, err = q.processedChunks(math.MaxInt64)
		if err != nil {
			return nil, false, err
		}
	}
	if len(tombstones) > 0 {
		return tombstones, false, nil
	}
	q.retained = false
	return nil, true, nil
}

// cleanupProcessedChunks scans the queue directory for tombstone files
// (.bin.processed) and deletes both the tombstone and its corresponding chunk file.
// This is called on startup (crash recovery) and when disabling maintenance mode.
func (q *DiskQueue) cleanupProcessedChunks() error {
	tombstones, err := q.processedChunks(math.MaxInt64)
	if err != nil {
		return err
	}
	q.logRemoveProcessedChunks(tombstones)
	return nil
}

// logRemoveProcessedChunks removes tombstoned chunks. A file it cannot remove
// is logged and left for the next cleanup.
func (q *DiskQueue) logRemoveProcessedChunks(tombstones []string) {
	err := removeProcessedChunks(tombstones)
	if err != nil {
		q.Logger.Warnf("failed to remove processed chunks, will retry at the next cleanup: %v", err)
	}
}

// processedChunks lists the tombstones of the chunks numbered below seq.
func (q *DiskQueue) processedChunks(seq int64) ([]string, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		if stderrors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "failed to read directory during tombstone cleanup")
	}

	var tombstones []string
	for _, entry := range entries {
		if !processedChunkFilePattern.MatchString(entry.Name()) {
			continue
		}
		tombstonePath := filepath.Join(q.dir, entry.Name())
		n, ok := chunkSeq(strings.TrimSuffix(tombstonePath, ".processed"))
		if ok && n >= seq {
			continue
		}
		tombstones = append(tombstones, tombstonePath)
	}
	return tombstones, nil
}

// removeProcessedChunks removes each tombstoned chunk, then its tombstone: a
// chunk without one is processed again.
func removeProcessedChunks(tombstones []string) error {
	var errs []error
	for _, tombstonePath := range tombstones {
		err := removeFile(strings.TrimSuffix(tombstonePath, ".processed"))
		if err != nil && !stderrors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
			continue
		}
		err = removeFile(tombstonePath)
		if err != nil && !stderrors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return stderrors.Join(errs...)
}

func (q *DiskQueue) removeChunk(path string) error {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if q.keepsProcessed() {
		tombstonePath := path + ".processed"
		f, err := os.Create(tombstonePath)
		if err == nil {
			_ = f.Close()
			q.forgetChunk(path)
			return nil
		}
		// a retention's chunks are needed for a replay: retry instead
		if q.retained {
			return errors.Wrap(err, "failed to create tombstone")
		}
		q.Logger.WithField("file", path).Errorf("failed to create tombstone, falling back to deletion: %v", err)
		// fall through to normal deletion
	}

	// the chunk stays in the queue until it is removed, so a failure is
	// retried
	err := os.Remove(path)
	if err != nil && !stderrors.Is(err, fs.ErrNotExist) {
		return errors.Wrap(err, "failed to remove chunk")
	}
	q.forgetChunk(path)
	return nil
}

// finishChunk removes a chunk whose records are processed, or quarantines it
// if it was corrupt.
func (q *DiskQueue) finishChunk(path string, corrupt error) error {
	if corrupt != nil {
		return q.quarantineChunk(path, corrupt)
	}
	return q.removeChunk(path)
}

// forgetChunk takes a chunk out of the queue and its accounting: a chunk is
// counted exactly as long as it is in the reader's list, with the values it
// was counted with. The caller holds rmLock.
func (q *DiskQueue) forgetChunk(path string) {
	ref, ok := q.r.forget(path)
	if !ok {
		return
	}

	q.m.Lock()
	q.recordCount -= ref.count
	q.diskUsage -= int64(ref.size)
	q.metrics.DiskUsage(q.diskUsage)
	q.metrics.Size(q.recordCount)
	q.m.Unlock()

	if q.onChunkRemoved != nil {
		q.onChunkRemoved()
	}
}

// quarantineChunk renames a torn or corrupt chunk file to <name>.corrupt so
// it is no longer scheduled, and removes its records from the queue's
// accounting so the queue can drain. The file is kept on disk for inspection.
func (q *DiskQueue) quarantineChunk(path string, cause error) error {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	// the chunk stays in the queue until it is renamed, so a failure is
	// retried
	quarantinePath := path + ".corrupt"
	kept := true
	var err error
	if q.keepsProcessed() {
		kept, err = q.quarantineKeptChunk(path, quarantinePath)
	} else {
		err = os.Rename(path, quarantinePath)
	}
	if stderrors.Is(err, fs.ErrNotExist) {
		q.forgetChunk(path)
		q.Logger.WithField("file", path).Errorf("corrupt chunk file is missing, nothing to quarantine: %v", cause)
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to quarantine corrupt chunk")
	}
	q.forgetChunk(path)

	if !kept {
		q.Logger.WithField("file", path).
			Errorf("chunk is truncated or corrupt, it will be removed once no backup or retention keeps it, without a quarantined copy; its unreadable records are lost: %v", cause)
		return nil
	}
	q.Logger.WithField("file", quarantinePath).
		Errorf("chunk is truncated or corrupt, quarantined it; its unreadable records are lost: %v", cause)
	return nil
}

// removeFile is a variable so tests can hold a removal to see what it blocks.
var removeFile = os.Remove

// hardLink is a variable so tests can simulate a filesystem without hard
// links.
var hardLink = os.Link

// quarantineKeptChunk quarantines a chunk that a backup or a retention keeps:
// the quarantined copy is a hard link, and the chunk itself is removed with
// the processed ones once nothing keeps them. It reports whether the
// quarantined copy was kept.
func (q *DiskQueue) quarantineKeptChunk(path, quarantinePath string) (kept bool, err error) {
	// a leftover from an interrupted attempt
	_ = os.Remove(quarantinePath)

	linkErr := hardLink(path, quarantinePath)
	if stderrors.Is(linkErr, fs.ErrNotExist) {
		return false, linkErr
	}
	if linkErr != nil {
		q.Logger.WithField("file", path).
			Warnf("cannot hard link the corrupt chunk, it will be removed once no backup or retention keeps it instead of kept: %v", linkErr)
	}

	f, err := os.Create(path + ".processed")
	if err != nil {
		if linkErr == nil {
			_ = os.Remove(quarantinePath)
		}
		return false, err
	}
	return linkErr == nil, f.Close()
}

// analyzeDisk is a slow method that determines the number of records
// stored on disk and in the partial chunk by reading the header of all the files in the directory.
// It also calculates the disk usage.
// It is used when the queue is first initialized.
func (q *DiskQueue) analyzeDisk() ([]chunkRef, error) {
	q.m.Lock()
	defer q.m.Unlock()

	// crash recovery: clean up any tombstones left from a previous maintenance mode session
	if err := q.cleanupProcessedChunks(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read directory")
	}

	chunkList := make([]chunkRef, 0, len(entries))
	// chunks holding nothing but a header, and the last chunk, which the
	// writer resumes
	var headerOnly []string
	var last string

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

		count, err := q.readChunkRecordCount(filePath)
		if err != nil {
			// a crash may have left a chunk file with an unreadable header.
			// It must not prevent the queue from starting.
			if err := q.salvageUnreadableChunk(filePath, fi.Size(), err); err != nil {
				return nil, err
			}
			continue
		}

		last = filePath

		// partial chunk, counted once the writer opened it
		if count == 0 {
			if fi.Size() == int64(chunkHeaderSize) {
				headerOnly = append(headerOnly, filePath)
			}
			continue
		}

		q.recordCount += count
		q.diskUsage += fi.Size()

		chunkList = append(chunkList, chunkRef{path: filePath, count: count, size: uint64(fi.Size())})
		continue
	}

	// an empty chunk the writer does not resume is never read, e.g. one a
	// backup sealed before a restart: remove it
	for _, path := range headerOnly {
		if path == last {
			continue
		}
		err := os.Remove(path)
		if err != nil {
			q.Logger.WithField("file", path).Warnf("failed to remove empty chunk: %v", err)
		}
	}

	return chunkList, nil
}

// salvageUnreadableChunk handles a chunk file whose header cannot be parsed,
// typically the leftover of a crash (power loss, disk full) while the chunk
// was being created.
// A file too short to contain a full header provably holds no records
// (records are only ever written after the header) and is removed.
// A full-size file with unreadable framing is renamed to <name>.corrupt so it
// no longer blocks startup but remains available for inspection.
// Only errors that are evidence of a corrupt or torn header are salvaged.
// Anything else is returned and fails initialization: an environmental error
// (e.g. permissions, I/O) does not mean the chunk is bad, and a well-formed
// header with an unknown version was likely written by a newer version of
// weaviate, so removing or hiding the chunk in those cases could drop valid
// tasks.
func (q *DiskQueue) salvageUnreadableChunk(path string, size int64, cause error) error {
	if !errors.Is(cause, errBadMagic) &&
		!errors.Is(cause, io.EOF) &&
		!errors.Is(cause, io.ErrUnexpectedEOF) {
		return cause
	}

	if size < int64(chunkHeaderSize) {
		if err := os.Remove(path); err != nil {
			return errors.Wrap(err, "failed to remove chunk with incomplete header")
		}
		q.Logger.WithField("file", path).
			Warnf("removed chunk with incomplete header (%d bytes), likely due to a crash: %v", size, cause)
		return nil
	}

	quarantinePath := path + ".corrupt"
	if err := os.Rename(path, quarantinePath); err != nil {
		return errors.Wrap(err, "failed to quarantine corrupt chunk")
	}
	q.Logger.WithField("file", quarantinePath).
		Errorf("quarantined chunk with unreadable header, its tasks are lost: %v", cause)
	return nil
}

// ListFiles returns a list of all chunk files in the queue directory.
// The returned paths are relative to the basePath.
// It is used for backup purposes and must be called only when the queue is not in use.
func (q *DiskQueue) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	q.m.Lock()
	defer q.m.Unlock()

	return q.listFilesNoLock(ctx, basePath)
}

func (q *DiskQueue) listFilesNoLock(ctx context.Context, basePath string) ([]string, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		if stderrors.Is(err, fs.ErrNotExist) {
			return []string{}, nil
		}

		return nil, errors.Wrap(err, "failed to read directory")
	}

	// build set of already-processed chunk paths to exclude from the backup list
	processedFiles := make(map[string]struct{})
	for _, entry := range entries {
		if processedChunkFilePattern.MatchString(entry.Name()) {
			chunkName := strings.TrimSuffix(entry.Name(), ".processed")
			processedFiles[filepath.Join(q.dir, chunkName)] = struct{}{}
		}
	}

	chunkList := make([]string, 0, len(entries))

	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if entry.IsDir() {
			continue
		}

		// check if the entry name matches the regex pattern of a chunk file.
		// This also excludes derived files such as .processed tombstones and
		// .corrupt quarantined chunks, since the pattern is anchored.
		if !chunkFilePattern.MatchString(entry.Name()) {
			continue
		}

		filePath := filepath.Join(q.dir, entry.Name())
		if _, ok := processedFiles[filePath]; ok {
			// already processed during maintenance mode, skip for backup
			continue
		}

		relPath, err := filepath.Rel(basePath, filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to get relative path: %w", err)
		}

		chunkList = append(chunkList, relPath)
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

func openChunk(path string) (_ *chunk, err error) {
	c := chunk{
		path: path,
	}

	c.f, err = os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			c.discard()
		}
	}()

	stat, err := c.f.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		return nil, errEmptyChunk
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

func chunkFromFile(f *os.File) (_ *chunk, err error) {
	c := chunk{
		path: f.Name(),
		f:    f,
	}
	defer func() {
		if err != nil {
			c.discard()
		}
	}()

	info, err := c.f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat chunk file")
	}
	if info.Size() == 0 {
		return nil, errEmptyChunk
	}
	c.size = uint64(info.Size())

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

	return &c, nil
}

// nextRecord reads the next record of a chunk, into buf when it fits. It returns no
// record at the end of the chunk, and with damaged set if the chunk ended early.
func (c *chunk) nextRecord(buf []byte) (record []byte, damaged error, err error) {
	buf = buf[:4]

	// read the record length
	_, err = io.ReadFull(c.r, buf)
	if errors.Is(err, io.EOF) {
		return nil, nil, nil
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, errors.Wrap(err, "chunk ends mid-record"), nil
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to read record length")
	}
	length := binary.BigEndian.Uint32(buf)
	if length == 0 {
		return nil, errors.New("invalid record length"), nil
	}
	// the length prefix comes from disk and may be corrupt: a record
	// cannot be larger than the chunk payload that contains it. Validate
	// before allocating.
	payloadSize := c.size - uint64(chunkHeaderSize)
	if uint64(length) > payloadSize-4 {
		return nil, errors.Errorf("invalid record length %d, chunk payload is only %d bytes", length, payloadSize), nil
	}

	// read the record
	if cap(buf) < int(length) {
		buf = make([]byte, length)
	} else {
		buf = buf[:length]
	}
	_, err = io.ReadFull(c.r, buf)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, errors.Wrap(err, "chunk ends mid-record"), nil
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to read record")
	}
	return buf, nil, nil
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

// discard releases the file and reader of a chunk that failed to open.
func (c *chunk) discard() {
	if c.f != nil {
		_ = c.f.Close()
		c.f = nil
	}
	if c.r != nil {
		readerPool.Put(c.r)
		c.r = nil
	}
}

func readChunkHeader(r io.Reader) (uint64, error) {
	// read the header
	header := make([]byte, chunkHeaderSize)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read header")
	}

	// check the magic number
	if !bytes.Equal(header[:len(magicHeader)], []byte(magicHeader)) {
		return 0, errBadMagic
	}

	// check the version
	if header[len(magicHeader)] != 1 {
		return 0, errUnknownVersion
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
	w           lazyBufferedWriter
	f           *os.File
	size        uint64
	recordCount uint64
	buf         [8]byte

	reader *chunkReader
	// lastSeq is the highest chunk number handed out, so names only grow
	// even if the clock steps back
	lastSeq int64
}

func newChunkWriter(dir string, reader *chunkReader, logger logrus.FieldLogger, maxSize uint64) (*chunkWriter, error) {
	ch := &chunkWriter{
		dir:     dir,
		reader:  reader,
		logger:  logger,
		maxSize: maxSize,
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
	var errs []error

	err := w.w.Flush()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush buffer"))
	}
	w.w.Release()

	if w.f != nil {
		err = w.f.Sync()
		if err != nil {
			errs = append(errs, errors.Wrap(err, "failed to sync"))
		}

		err := w.f.Close()
		if err != nil {
			errs = append(errs, errors.Wrap(err, "failed to close chunk"))
		}

		w.f = nil
	}

	return stderrors.Join(errs...)
}

// chunkTimeNow returns the timestamp used to name new chunk files.
// It is a variable so tests can force a timestamp collision.
var chunkTimeNow = func() int64 { return time.Now().UnixMicro() }

// createChunkMaxAttempts bounds the number of names Create tries before
// giving up. In practice a single retry is already rare: it requires two
// chunks created within the same microsecond.
const createChunkMaxAttempts = 1000

func (w *chunkWriter) Create() error {
	var err error

	// Chunk files are named after their creation time in microseconds, so
	// two chunks created within the same microsecond collide on the same
	// name. Without O_EXCL the second create would silently reopen the
	// existing chunk and later header writes would clobber it. Create
	// exclusively and bump the timestamp until a free name is found.
	//
	// Names only grow, which is load-bearing: the filename doubles as the
	// replay ordering key (chunks are reloaded in name order, and a
	// retention's start is a number in that order).
	ts := w.nextSeq()
	for i := 0; ; i++ {
		path := chunkPath(w.dir, ts)
		w.f, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return errors.Wrap(err, "failed to create chunk file")
		}
		// i+1 attempts have failed at this point, so this gives up after
		// exactly createChunkMaxAttempts tries
		if i+1 >= createChunkMaxAttempts {
			return errors.Wrapf(err, "failed to create chunk file after %d attempts", createChunkMaxAttempts)
		}
		ts++
	}
	w.lastSeq = ts

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
	w.size = uint64(chunkHeaderSize)

	return nil
}

// nextSeq hands out the number of the next chunk name: the clock, or one
// past the last number if the clock is behind it.
func (w *chunkWriter) nextSeq() int64 {
	seq := chunkTimeNow()
	if seq <= w.lastSeq {
		seq = w.lastSeq + 1
	}
	w.lastSeq = seq
	return seq
}

func (w *chunkWriter) Open() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return errors.Wrap(err, "failed to read directory")
	}

	// adopt the most recent chunk file as the write target. The directory may
	// also contain derived files (e.g. .corrupt quarantined chunks): those
	// must never be written to.
	var lastChunk string
	for _, entry := range entries {
		if entry.IsDir() || !chunkFilePattern.MatchString(entry.Name()) {
			continue
		}
		lastChunk = entry.Name()
		seq, ok := chunkSeq(lastChunk)
		if ok && seq > w.lastSeq {
			w.lastSeq = seq
		}
	}

	if lastChunk == "" {
		return nil
	}

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
		w.size = uint64(chunkHeaderSize)

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

	err = w.reader.PromoteChunk(w.f, w.recordCount, w.size)
	if err != nil {
		return errors.Wrap(err, "failed to promote chunk")
	}

	w.f = nil
	w.size = 0
	w.recordCount = 0
	// the buffer is empty here: give it back so idle queues hold no memory
	w.w.Release()

	return nil
}

type chunkReader struct {
	m   sync.Mutex
	dir string
	// chunks not processed yet, oldest first. A chunk leaves the list when
	// it is removed, so a batch that is not done is read again.
	chunkList []chunkRef
	chunks    map[string]*os.File
}

// chunkRef is a chunk as counted by the queue, known even when its header
// can no longer be read.
type chunkRef struct {
	path  string
	count uint64
	size  uint64
	// set once the chunk's records are processed but its file could not be
	// removed, or quarantined if corrupt is set
	processed bool
	corrupt   error
}

func newChunkReader(dir string, chunkList []chunkRef) *chunkReader {
	return &chunkReader{
		dir:       dir,
		chunks:    make(map[string]*os.File),
		chunkList: chunkList,
	}
}

// ReadChunk returns the oldest chunk not processed yet, without removing it.
func (r *chunkReader) ReadChunk() (*chunk, error) {
	r.m.Lock()
	if len(r.chunkList) == 0 {
		r.m.Unlock()
		return nil, nil
	}
	ref := r.chunkList[0]
	if ref.processed {
		r.m.Unlock()
		return nil, &chunkReadError{ref: ref, err: errChunkProcessed}
	}
	f, ok := r.chunks[ref.path]
	// the chunk closes the handle once read
	delete(r.chunks, ref.path)
	r.m.Unlock()

	var c *chunk
	var err error
	if ok {
		c, err = chunkFromFile(f)
	} else {
		c, err = openChunk(ref.path)
	}
	if err != nil {
		return nil, &chunkReadError{ref: ref, err: err}
	}
	return c, nil
}

// chunkReadError is returned by ReadChunk when the oldest chunk cannot be
// read. The chunk stays in the list.
type chunkReadError struct {
	ref chunkRef
	err error
}

func (e *chunkReadError) Error() string {
	return fmt.Sprintf("failed to read chunk %s: %v", e.ref.path, e.err)
}

func (e *chunkReadError) Unwrap() error { return e.err }

// markProcessed keeps a chunk whose records are processed in the list, so
// its removal is retried without reading it again.
func (r *chunkReader) markProcessed(path string, corrupt error) {
	r.m.Lock()
	defer r.m.Unlock()

	i := slices.IndexFunc(r.chunkList, func(ref chunkRef) bool { return ref.path == path })
	if i >= 0 {
		r.chunkList[i].processed = true
		r.chunkList[i].corrupt = corrupt
	}
}

// rewind puts kept chunks back in name order, and makes the chunks still
// listed as processed readable again. A chunk can be listed and tombstoned at
// once when a tombstone failed halfway; it returns the chunks it added.
func (r *chunkReader) rewind(refs []chunkRef) []chunkRef {
	r.m.Lock()
	defer r.m.Unlock()

	listed := make(map[string]struct{}, len(r.chunkList))
	for i := range r.chunkList {
		r.chunkList[i].processed = false
		r.chunkList[i].corrupt = nil
		listed[r.chunkList[i].path] = struct{}{}
	}
	added := make([]chunkRef, 0, len(refs))
	for _, ref := range refs {
		if _, ok := listed[ref.path]; !ok {
			added = append(added, ref)
		}
	}
	r.chunkList = append(r.chunkList, added...)
	slices.SortFunc(r.chunkList, func(a, b chunkRef) int {
		return strings.Compare(a.path, b.path)
	})
	return added
}

// firstUnprocessed is the path of the oldest chunk whose records are not
// processed yet, or empty.
func (r *chunkReader) firstUnprocessed() string {
	r.m.Lock()
	defer r.m.Unlock()
	for _, ref := range r.chunkList {
		if !ref.processed {
			return ref.path
		}
	}
	return ""
}

// forget removes a chunk from the list of chunks to read, and returns it if
// it was there.
func (r *chunkReader) forget(path string) (chunkRef, bool) {
	r.m.Lock()
	defer r.m.Unlock()

	i := slices.IndexFunc(r.chunkList, func(ref chunkRef) bool { return ref.path == path })
	if i < 0 {
		return chunkRef{}, false
	}
	ref := r.chunkList[i]
	if i == 0 {
		// the common case: processed chunks leave from the front. Reslicing
		// avoids shifting the whole backlog; append reclaims the space once
		// the list grows.
		r.chunkList[0] = chunkRef{}
		r.chunkList = r.chunkList[1:]
	} else {
		r.chunkList = slices.Delete(r.chunkList, i, i+1)
	}
	return ref, true
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

func (r *chunkReader) PromoteChunk(f *os.File, count, size uint64) error {
	ref := chunkRef{path: f.Name(), count: count, size: size}

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
		r.chunkList = append(r.chunkList, ref)
		r.m.Unlock()

		return nil
	}
	defer r.m.Unlock()

	r.chunks[f.Name()] = f
	r.chunkList = append(r.chunkList, ref)

	return nil
}

// compile time check for Queue interface
var _ = Queue(new(DiskQueue))

// lazyBufferedWriter is a bufio.Writer that initializes
// the underlying buffer only when the first Write is called.
type lazyBufferedWriter struct {
	w *bufio.Writer
	f *os.File
}

func (w *lazyBufferedWriter) Write(p []byte) (nn int, err error) {
	if w.w == nil {
		w.w = getBufioWriter(w.f)
	}

	return w.w.Write(p)
}

func (w *lazyBufferedWriter) Flush() error {
	if w.w == nil {
		return nil
	}

	return w.w.Flush()
}

func (w *lazyBufferedWriter) Reset(f *os.File) {
	w.f = f
	if w.w != nil {
		w.w.Reset(f)
	}
}

func (w *lazyBufferedWriter) WriteByte(c byte) error {
	if w.w == nil {
		w.w = getBufioWriter(w.f)
	}

	return w.w.WriteByte(c)
}

// Release returns the buffered writer to the pool. Callers must flush first:
// the pool resets the writer and drops anything still buffered.
// The lazyBufferedWriter can still be used after calling Release(),
// but a new bufio.Writer will be allocated on the next Write.
func (w *lazyBufferedWriter) Release() {
	if w.w != nil {
		buf := w.w
		w.w = nil
		putBufioWriter(buf)
	}
}

var bufioWriterPool = sync.Pool{
	New: func() any {
		return bufio.NewWriterSize(nil, chunkWriterBufferSize)
	},
}

func getBufioWriter(f *os.File) *bufio.Writer {
	w := bufioWriterPool.Get().(*bufio.Writer)
	if f != nil {
		w.Reset(f)
	}
	return w
}

func putBufioWriter(w *bufio.Writer) {
	w.Reset(nil)
	bufioWriterPool.Put(w)
}

// chunkPath is the path of the chunk numbered seq in dir.
func chunkPath(dir string, seq int64) string {
	return filepath.Join(dir, fmt.Sprintf(chunkFileFmt, seq))
}

// chunkSeq is the number a chunk file is named after.
func chunkSeq(path string) (int64, bool) {
	name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "chunk-"), ".bin")
	seq, err := strconv.ParseInt(name, 10, 64)
	return seq, err == nil
}
