//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"context"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/sirupsen/logrus"
)

// Store groups multiple buckets together, it "owns" one folder on the file
// system
type Store struct {
	rootDir       string
	bucketsByName map[string]*Bucket
	logger        logrus.FieldLogger
	metrics       *Metrics

	// Prevent concurrent manipulations to the bucketsByNameMap, most notably
	// when initializing buckets in parallel
	bucketAccessLock sync.RWMutex
}

func New(rootDir string, logger logrus.FieldLogger,
	metrics *Metrics,
) (*Store, error) {
	s := &Store{
		rootDir:       rootDir,
		bucketsByName: map[string]*Bucket{},
		logger:        logger,
		metrics:       metrics,
	}

	return s, s.init()
}

func (s *Store) Bucket(name string) *Bucket {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	return s.bucketsByName[name]
}

func (s *Store) UpdateBucketsStatus(targetStatus storagestate.Status) {
	// UpdateBucketsStatus is a write operation on the bucket itself, but from
	// the perspective of our bucket access map this is a read-only operation,
	// hence an RLock()
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	for _, b := range s.bucketsByName {
		if b == nil {
			continue
		}

		b.UpdateStatus(targetStatus)
	}

	if targetStatus == storagestate.StatusReadOnly {
		s.logger.WithField("action", "lsm_compaction").
			WithField("path", s.rootDir).
			Warn("compaction halted due to shard READONLY status")
	}
}

func (s *Store) init() error {
	if err := os.MkdirAll(s.rootDir, 0o700); err != nil {
		return err
	}

	return nil
}

func (s *Store) bucketDir(bucketName string) string {
	return path.Join(s.rootDir, bucketName)
}

func (s *Store) CreateOrLoadBucket(ctx context.Context, bucketName string,
	opts ...BucketOption,
) error {
	if b := s.Bucket(bucketName); b != nil {
		return nil
	}

	b, err := NewBucket(ctx, s.bucketDir(bucketName), s.logger, s.metrics, opts...)
	if err != nil {
		return err
	}

	s.setBucket(bucketName, b)
	return nil
}

func (s *Store) setBucket(name string, b *Bucket) {
	s.bucketAccessLock.Lock()
	defer s.bucketAccessLock.Unlock()

	s.bucketsByName[name] = b
}

func (s *Store) Shutdown(ctx context.Context) error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	for name, bucket := range s.bucketsByName {
		if err := bucket.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown bucket %q", name)
		}
	}

	return nil
}

func (s *Store) WriteWALs() error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	for name, bucket := range s.bucketsByName {
		if err := bucket.WriteWAL(); err != nil {
			return errors.Wrapf(err, "bucket %q", name)
		}
	}

	return nil
}

// bucketJobStatus is used to safely track the status of
// a job applied to each of a store's buckets when run
// in parallel
type bucketJobStatus struct {
	sync.Mutex
	buckets map[*Bucket]error
}

func newBucketJobStatus() *bucketJobStatus {
	return &bucketJobStatus{
		buckets: make(map[*Bucket]error),
	}
}

type jobFunc func(context.Context, *Bucket) (interface{}, error)

type rollbackFunc func(context.Context, *Bucket) error

func (s *Store) PauseCompaction(ctx context.Context) error {
	pauseCompaction := func(ctx context.Context, b *Bucket) (interface{}, error) {
		return nil, b.PauseCompaction(ctx)
	}

	resumeCompaction := func(ctx context.Context, b *Bucket) error {
		return b.ResumeCompaction(ctx)
	}

	_, err := s.runJobOnBuckets(ctx, pauseCompaction, resumeCompaction)
	return err
}

func (s *Store) FlushMemtables(ctx context.Context) error {
	flushMemtable := func(ctx context.Context, b *Bucket) (interface{}, error) {
		return nil, b.FlushMemtable(ctx)
	}

	_, err := s.runJobOnBuckets(ctx, flushMemtable, nil)
	return err
}

func (s *Store) ListFiles(ctx context.Context) ([]string, error) {
	listFiles := func(ctx context.Context, b *Bucket) (interface{}, error) {
		return b.ListFiles(ctx)
	}

	result, err := s.runJobOnBuckets(ctx, listFiles, nil)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, res := range result {
		files = append(files, res.([]string)...)
	}

	return files, nil
}

// runJobOnBuckets applies a jobFunc to each bucket in the store in parallel.
// The jobFunc allows for the job to return an arbitrary value.
// Additionally, a rollbackFunc may be provided which will be run on the target
// bucket in the event of an unsuccessful job.
func (s *Store) runJobOnBuckets(ctx context.Context, jobFunc jobFunc,
	rollbackFunc rollbackFunc,
) ([]interface{}, error) {
	var (
		status      = newBucketJobStatus()
		resultQueue = make(chan interface{}, len(s.bucketsByName))
		wg          = sync.WaitGroup{}
	)

	for _, bucket := range s.bucketsByName {
		wg.Add(1)
		b := bucket
		go func() {
			status.Lock()
			defer status.Unlock()
			res, err := jobFunc(ctx, b)
			resultQueue <- res
			status.buckets[b] = err
			wg.Done()
		}()
	}

	wg.Wait()
	close(resultQueue)

	var errs errorcompounder.ErrorCompounder
	for _, err := range status.buckets {
		errs.Add(err)
	}

	if errs.Len() != 0 {
		// if any of the bucket jobs failed, and a
		// rollbackFunc has been provided, attempt
		// to roll back. if this fails, the err is
		// added to the compounder
		for b, jobErr := range status.buckets {
			if jobErr != nil && rollbackFunc != nil {
				if rollbackErr := rollbackFunc(ctx, b); rollbackErr != nil {
					errs.AddWrap(rollbackErr, "bucket job rollback")
				}
			}
		}

		return nil, errs.ToError()
	}

	var finalResult []interface{}
	for res := range resultQueue {
		finalResult = append(finalResult, res)
	}

	return finalResult, nil
}
