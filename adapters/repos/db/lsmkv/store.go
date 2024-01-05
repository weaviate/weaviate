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
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// Store groups multiple buckets together, it "owns" one folder on the file
// system
type Store struct {
	dir           string
	rootDir       string
	bucketsByName map[string]*Bucket
	logger        logrus.FieldLogger
	metrics       *Metrics

	cycleCallbacks *storeCycleCallbacks

	// Prevent concurrent manipulations to the bucketsByNameMap, most notably
	// when initializing buckets in parallel
	bucketAccessLock sync.RWMutex
}

// New initializes a new [Store] based on the root dir. If state is present on
// disk, it is loaded, if the folder is empty a new store is initialized in
// there.
func New(dir, rootDir string, logger logrus.FieldLogger, metrics *Metrics,
	shardCompactionCallbacks, shardFlushCallbacks cyclemanager.CycleCallbackGroup,
) (*Store, error) {
	s := &Store{
		dir:           dir,
		rootDir:       rootDir,
		bucketsByName: map[string]*Bucket{},
		logger:        logger,
		metrics:       metrics,
	}
	s.initCycleCallbacks(shardCompactionCallbacks, shardFlushCallbacks)

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
			WithField("path", s.dir).
			Warn("compaction halted due to shard READONLY status")
	}
}

func (s *Store) init() error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	return nil
}

func (s *Store) bucketDir(bucketName string) string {
	return path.Join(s.dir, bucketName)
}

// CreateOrLoadBucket registers a bucket with the given name. If state on disk
// exists for this bucket it is loaded, otherwise created. Pass [BucketOptions]
// to configure the strategy of a bucket. The strategy defaults to "replace".
// For example, to load or create a map-type bucket, do:
//
//	ctx := context.Background()
//	err := store.CreateOrLoadBucket(ctx, "my_bucket_name", WithStrategy(StrategyReplace))
//	if err != nil { /* handle error */ }
//
//	// you can now access the bucket using store.Bucket()
//	b := store.Bucket("my_bucket_name")
func (s *Store) CreateOrLoadBucket(ctx context.Context, bucketName string,
	opts ...BucketOption,
) error {
	if b := s.Bucket(bucketName); b != nil {
		return nil
	}

	b, err := NewBucket(ctx, s.bucketDir(bucketName), s.rootDir, s.logger, s.metrics,
		s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks, opts...)
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

func (s *Store) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	listFiles := func(ctx context.Context, b *Bucket) (interface{}, error) {
		basePath, err := filepath.Rel(basePath, b.dir)
		if err != nil {
			return nil, fmt.Errorf("bucket relative path: %w", err)
		}
		return b.ListFiles(ctx, basePath)
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
func (s *Store) runJobOnBuckets(ctx context.Context,
	jobFunc jobFunc, rollbackFunc rollbackFunc,
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

func (s *Store) GetBucketsByName() map[string]*Bucket {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	newMap := map[string]*Bucket{}
	for name, bucket := range s.bucketsByName {
		newMap[name] = bucket
	}

	return newMap
}

// Creates bucket, first removing any files if already exist
// Bucket can not be registered in bucketsByName before removal
func (s *Store) CreateBucket(ctx context.Context, bucketName string,
	opts ...BucketOption,
) error {
	if b := s.Bucket(bucketName); b != nil {
		return fmt.Errorf("bucket %s exists and is already in use", bucketName)
	}

	bucketDir := s.bucketDir(bucketName)
	if err := os.RemoveAll(bucketDir); err != nil {
		return errors.Wrapf(err, "failed removing bucket %s files", bucketName)
	}

	b, err := NewBucket(ctx, bucketDir, s.rootDir, s.logger, s.metrics,
		s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks, opts...)
	if err != nil {
		return err
	}

	s.setBucket(bucketName, b)
	return nil
}

// Replaces 1st bucket with 2nd one. Both buckets have to registered in bucketsByName.
// 2nd bucket swaps the 1st one in bucketsByName using 1st one's name, 2nd one's name is deleted.
// Dir path of 2nd bucket is changed to dir of 1st bucket as well as all other related paths of
// bucket resources (segment group, memtables, commit log).
// Dir path of 1st bucket is temporarily suffixed with "___del", later on bucket is shutdown and
// its files deleted.
// 2nd bucket becomes 1st bucket
func (s *Store) ReplaceBuckets(ctx context.Context, bucketName, replacementBucketName string) error {
	s.bucketAccessLock.Lock()
	defer s.bucketAccessLock.Unlock()

	bucket := s.bucketsByName[bucketName]
	if bucket == nil {
		return fmt.Errorf("bucket '%s' not found", bucketName)
	}
	replacementBucket := s.bucketsByName[replacementBucketName]
	if replacementBucket == nil {
		return fmt.Errorf("replacement bucket '%s' not found", replacementBucketName)
	}
	s.bucketsByName[bucketName] = replacementBucket
	delete(s.bucketsByName, replacementBucketName)

	currBucketDir := bucket.dir
	newBucketDir := bucket.dir + "___del"
	currReplacementBucketDir := replacementBucket.dir
	newReplacementBucketDir := currBucketDir

	if err := os.Rename(currBucketDir, newBucketDir); err != nil {
		return errors.Wrapf(err, "failed moving orig bucket dir '%s'", currBucketDir)
	}
	if err := os.Rename(currReplacementBucketDir, newReplacementBucketDir); err != nil {
		return errors.Wrapf(err, "failed moving replacement bucket dir '%s'", currReplacementBucketDir)
	}

	s.updateBucketDir(bucket, currBucketDir, newBucketDir)
	s.updateBucketDir(replacementBucket, currReplacementBucketDir, newReplacementBucketDir)

	if err := bucket.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "failed shutting down bucket old '%s'", bucketName)
	}
	if err := os.RemoveAll(newBucketDir); err != nil {
		return errors.Wrapf(err, "failed removing dir '%s'", newBucketDir)
	}

	return nil
}

func (s *Store) RenameBucket(ctx context.Context, bucketName, newBucketName string) error {
	s.bucketAccessLock.Lock()
	defer s.bucketAccessLock.Unlock()

	currBucket := s.bucketsByName[bucketName]
	if currBucket == nil {
		return fmt.Errorf("bucket '%s' not found", bucketName)
	}
	newBucket := s.bucketsByName[newBucketName]
	if newBucket != nil {
		return fmt.Errorf("bucket '%s' already exists", newBucketName)
	}
	s.bucketsByName[newBucketName] = currBucket
	delete(s.bucketsByName, bucketName)

	currBucketDir := currBucket.dir
	newBucketDir := s.bucketDir(newBucketName)

	if err := os.Rename(currBucketDir, newBucketDir); err != nil {
		return errors.Wrapf(err, "failed renaming bucket dir '%s' to '%s'", currBucketDir, newBucketDir)
	}

	s.updateBucketDir(currBucket, currBucketDir, newBucketDir)
	return nil
}

func (s *Store) updateBucketDir(bucket *Bucket, bucketDir, newBucketDir string) {
	updatePath := func(src string) string {
		return strings.Replace(src, bucketDir, newBucketDir, 1)
	}

	bucket.flushLock.Lock()
	bucket.dir = newBucketDir
	if bucket.active != nil {
		bucket.active.path = updatePath(bucket.active.path)
		bucket.active.commitlog.path = updatePath(bucket.active.commitlog.path)
	}
	if bucket.flushing != nil {
		bucket.flushing.path = updatePath(bucket.flushing.path)
		bucket.flushing.commitlog.path = updatePath(bucket.flushing.commitlog.path)
	}
	bucket.flushLock.Unlock()

	bucket.disk.maintenanceLock.Lock()
	bucket.disk.dir = newBucketDir
	for _, segment := range bucket.disk.segments {
		segment.path = updatePath(segment.path)
	}
	bucket.disk.maintenanceLock.Unlock()
}
