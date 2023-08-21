package db

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// IndexQueue is a persistent queue of vectors to index.
// It batches vectors together before sending them to the indexing worker.
// It persists the vectors on disk to ensure they are not lost in case of a crash.
// It is safe to use concurrently.
type IndexQueue struct {
	Index BatchIndexer

	logger logrus.FieldLogger

	maxQueueSize  int
	maxStaleTime  time.Duration
	retryInterval time.Duration

	// processCh is the channel used to send vectors to the indexing worker.
	processCh chan indexQueueJob

	// if closed, prevents new vectors from being added to the queue.
	closed chan struct{}

	// tracks the number of in-flight pushes
	wg sync.WaitGroup

	// tracks the background workers
	workerWg sync.WaitGroup

	// walFile is the append-only file used to persist the pending vectors.
	walFile   *os.File
	walBuffer *bufio.Writer

	// queue of not-yet-indexed vectors
	toIndex []indexQueueJob

	// keeps track of the last time the queue was indexed
	staleTm *time.Ticker
}

type IndexQueueOptions struct {
	// MaxQueueSize is the maximum number of vectors to queue
	// before sending them to the indexing worker.
	MaxQueueSize int

	// MaxStaleTime is the maximum time to wait before sending
	// the pending vectors to the indexing worker, regardless
	// of the queue size.
	MaxStaleTime time.Duration

	// Logger is the logger used by the queue.
	Logger logrus.FieldLogger

	// RetryInterval is the interval between retries when
	// indexing fails.
	RetryInterval time.Duration
}

type BatchIndexer interface {
	AddBatch(id []uint64, vector [][]float32) error
}

func NewIndexQueue(
	walPath string,
	index BatchIndexer,
	opts IndexQueueOptions,
) (*IndexQueue, error) {
	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}

	if opts.MaxQueueSize == 0 {
		opts.MaxQueueSize = 10_000
	}

	if opts.MaxStaleTime == 0 {
		opts.MaxStaleTime = 10 * time.Second
	}

	if opts.RetryInterval == 0 {
		opts.RetryInterval = 1 * time.Second
	}

	q := IndexQueue{
		Index:         index,
		maxQueueSize:  opts.MaxQueueSize,
		maxStaleTime:  opts.MaxStaleTime,
		retryInterval: opts.RetryInterval,
		processCh:     make(chan indexQueueJob),
		closed:        make(chan struct{}),
		toIndex:       make([]indexQueueJob, 0, opts.MaxQueueSize),
		logger:        opts.Logger.WithField("component", "index_queue"),
	}

	var err error

	// open append-only file
	q.walFile, err = os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open wal file")
	}

	q.walBuffer = bufio.NewWriter(q.walFile)

	q.workerWg.Add(1)
	go func() {
		defer q.workerWg.Done()

		q.processor()
	}()

	return &q, nil
}

// Close waits till the queue has ingested and persisted all pending vectors.
func (q *IndexQueue) Close() error {
	// check if the queue is closed
	select {
	case <-q.closed:
		return errors.New("index queue closed")
	default:
	}

	// prevent new jobs from being added
	close(q.closed)

	// wait for in-flight pushes to finish
	q.wg.Wait()

	close(q.processCh)

	q.workerWg.Wait()

	err := q.walBuffer.Flush()
	if err != nil {
		_ = q.walFile.Close()
		return errors.Wrap(err, "failed to flush wal buffer")
	}

	// close the wal file
	return q.walFile.Close()
}

type indexQueueJob struct {
	id     uint64
	vector []float32
	done   chan error
}

// Push adds a vector to the persistent indexing queue.
// It waits until the vector is successfully persisted to the
// on-disk queue or sent to the indexing worker.
func (q *IndexQueue) Push(ctx context.Context, id uint64, vector []float32) error {
	// check if the queue is closed
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.closed:
		return errors.New("index queue closed")
	default:
	}

	// count the number of in-flight pushes
	q.wg.Add(1)
	defer q.wg.Done()

	done := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.processCh <- indexQueueJob{
		id:     id,
		vector: vector,
		done:   done,
	}:
		return <-done
	}
}

// This is the processor worker. Its job is to batch jobs together before
// sending them to the index.
// While the queue is not full or stale, it persists the jobs on disk.
// Once the queue is full or stale, it sends the jobs to the indexing worker.
// It batches concurrent jobs to reduce the number of disk writes.
func (q *IndexQueue) processor() {
	received := make([]indexQueueJob, 0, q.maxQueueSize)

	q.staleTm = time.NewTicker(q.maxStaleTime)

	for {
		received = received[:0]

		select {
		case j := <-q.processCh:
			received = append(received, j)

			// loop over the channel to dequeue it until it's empty
			// or we have reached one of our thresholds
		LOOP:
			for {
				select {
				case jj := <-q.processCh:
					received = append(received, jj)
					if q.shouldIndex(received) {
						break LOOP
					}
				default:
					// the channel is empty, break out of the loop
					break LOOP
				}
			}

			// if the queue is not stale and not full,
			// persist it on disk
			if !q.shouldIndex(received) {
				err := q.persist(received)
				if err != nil {
					q.notifyError(received, err)
					continue
				}

				// add the new jobs to the toIndex buffer
				q.toIndex = append(q.toIndex, received...)

				// notify the jobs that they have been processed
				q.notifySuccess(received)

				continue
			}

			fmt.Println("should index")
			// we now need to index the vectors

			// skip the disk write and send the jobs to the indexing worker.
			// this will make the clients wait synchronously, but it will reduce
			// the number of disk writes, which should be overall faster.
			// TODO: confirm this assumption as it's only worth it
			// if the indexing is faster than the disk write.
			q.toIndex = append(q.toIndex, received...)

			err := q.indexVectors(q.toIndex)
			if err != nil {
				// if the queue is closed, abort.
				// best effort, try to persist the vectors
				_ = q.persist(received)

				q.notifyError(received, err)
				continue
			}

			// indexing was successful, notify the clients
			q.notifySuccess(received)

			// reset the on-disk queue
			err = q.reset()
			if err != nil {
				// TODO: if we can't reset the file
				// should we recreate it?

				q.notifyError(received, err)
				continue
			}
		case <-q.staleTm.C:
			// if the queue is stale, send the jobs to the indexing worker.
			if len(q.toIndex) == 0 {
				q.staleTm.Reset(q.maxStaleTime)
				continue
			}

			err := q.indexVectors(q.toIndex)
			if err != nil {
				// if the queue is closed, abort.
				continue
			}

			// reset the queue
			err = q.reset()
			if err != nil {
				continue
			}
		case <-q.closed:
			// if the queue is closed, do nothing.
			return
		}
	}
}

func (q *IndexQueue) shouldIndex(received []indexQueueJob) bool {
	// if the queue is not full, continue
	if len(received)+len(q.toIndex) >= q.maxQueueSize {
		return true
	}

	// if the queue is not stale, continue
	select {
	case <-q.staleTm.C:
		return true
	default:
		return false
	}
}

func (q *IndexQueue) persist(vectors []indexQueueJob) error {
	buf := make([]byte, 8)

	for _, jj := range vectors {
		// store the id only
		binary.BigEndian.PutUint64(buf, jj.id)
		_, err := q.walBuffer.Write(buf)
		if err != nil {
			jj.done <- err
			continue
		}
	}

	// write to the file
	err := q.walBuffer.Flush()
	if err != nil {
		return err
	}

	// ensure the data is persisted to disk
	err = q.walFile.Sync()
	if err != nil {
		return err
	}

	return nil
}
func (q *IndexQueue) indexVectors(toIndex []indexQueueJob) error {
	ids := make([]uint64, 0, len(toIndex))
	vectors := make([][]float32, 0, len(toIndex))

	for _, j := range toIndex {
		ids = append(ids, j.id)
		vectors = append(vectors, j.vector)
	}

	for {
		err := q.Index.AddBatch(ids, vectors)
		if err == nil {
			break
		}

		q.logger.WithError(err).Infof("failed to index vectors, retrying in %s", q.retryInterval.String())

		// TODO: if we can't index the vectors
		// we should maybe persist the additional vectors,
		// release the clients and retry.

		t := time.NewTimer(q.retryInterval)
		select {
		case <-q.closed:
			// drain the timer
			if !t.Stop() {
				<-t.C
			}
			return errors.New("index queue closed")
		case <-t.C:
		}
	}

	return nil
}

func (q *IndexQueue) notifyError(jobs []indexQueueJob, err error) {
	for _, j := range jobs {
		j.done <- err
	}
}

func (q *IndexQueue) notifySuccess(jobs []indexQueueJob) {
	q.notifyError(jobs, nil)
}

func (q *IndexQueue) reset() error {
	// reset the queue
	q.toIndex = q.toIndex[:0]

	// reset the stale timer
	q.staleTm.Reset(q.maxStaleTime)

	// reset the on-disk queue
	return q.resetFile()
}

func (q *IndexQueue) resetFile() error {
	_, err := q.walFile.Seek(0, 0)
	if err != nil {
		return errors.Wrap(err, "failed to reset wal file position")
	}

	err = q.walFile.Truncate(0)
	if err != nil {
		return errors.Wrap(err, "failed to truncate wal file")
	}

	// reset the buffer
	q.walBuffer.Reset(q.walFile)

	return nil
}
