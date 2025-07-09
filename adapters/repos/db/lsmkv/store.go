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
	"runtime"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storagestate"
	wsync "github.com/weaviate/weaviate/entities/sync"
)

var ErrAlreadyClosed = errors.New("store already closed")

// Store groups multiple buckets together, it "owns" one folder on the file
// system
type Store struct {
	dir     string
	rootDir string

	// Prevent concurrent manipulations to the bucketsByNameMap, most notably
	// when initializing buckets in parallel
	bucketAccessLock sync.RWMutex
	bucketsByName    map[string]*Bucket

	logger  logrus.FieldLogger
	metrics *Metrics

	cycleCallbacks *storeCycleCallbacks
	bcreator       BucketCreator
	// Prevent concurrent manipulations to the same Bucket, specially if there is
	// action on the bucket in the meantime.
	bucketsLocks *wsync.KeyLocker

	closeLock sync.RWMutex
	closed    bool
}

// New initializes a new [Store] based on the root dir. If state is present on
// disk, it is loaded, if the folder is empty a new store is initialized in
// there.
func New(dir, rootDir string, logger logrus.FieldLogger, metrics *Metrics,
	shardCompactionCallbacks, shardCompactionAuxCallbacks,
	shardFlushCallbacks cyclemanager.CycleCallbackGroup,
) (*Store, error) {
	s := &Store{
		dir:           dir,
		rootDir:       rootDir,
		bucketsByName: map[string]*Bucket{},
		bucketsLocks:  wsync.NewKeyLocker(),
		bcreator:      NewBucketCreator(),
		logger:        logger,
		metrics:       metrics,
	}
	s.initCycleCallbacks(shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)

	return s, s.init()
}

func (s *Store) Bucket(name string) *Bucket {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	return s.bucketsByName[name]
}

func (s *Store) UpdateBucketsStatus(targetStatus storagestate.Status) error {
	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	if s.closed {
		return fmt.Errorf("%w: updating buckets state in store %q", ErrAlreadyClosed, s.dir)
	}

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

	return nil
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
) (err error) {
	defer func() {
		p := recover()
		if p == nil {
			// happy path
			return
		}

		entsentry.Recover(p)

		err = fmt.Errorf("unexpected error loading bucket %q at path %q: %v",
			bucketName, s.rootDir, p)
		// logger is already annotated to identify the store (e.g. collection +
		// shard), we only need to annotate it with the exact path of this
		// bucket.
		s.logger.
			WithFields(logrus.Fields{
				"action":   "lsm_create_or_load_bucket",
				"root_dir": s.rootDir,
				"dir":      s.dir,
				"bucket":   bucketName,
			}).
			WithError(err).Errorf("unexpected error loading shard")
		debug.PrintStack()
	}()

	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	if s.closed {
		return fmt.Errorf("%w: adding a bucket %q to store %q", ErrAlreadyClosed, bucketName, s.dir)
	}

	s.bucketsLocks.Lock(bucketName)
	defer s.bucketsLocks.Unlock(bucketName)

	if b := s.Bucket(bucketName); b != nil {
		return nil
	}

	compactionCallbacks := s.cycleCallbacks.compactionCallbacks
	if bucketName == helpers.ObjectsBucketLSM && s.cycleCallbacks.compactionAuxCallbacks != nil {
		compactionCallbacks = s.cycleCallbacks.compactionAuxCallbacks
	}

	// bucket can be concurrently loaded with another buckets but
	// the same bucket will be loaded only once
	b, err := s.bcreator.NewBucket(ctx, s.bucketDir(bucketName), s.rootDir, s.logger, s.metrics,
		compactionCallbacks, s.cycleCallbacks.flushCallbacks, opts...)
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
	s.closeLock.Lock()
	defer s.closeLock.Unlock()

	if s.closed {
		return fmt.Errorf("%w: closing store %q", ErrAlreadyClosed, s.dir)
	}

	s.closed = true

	s.bucketAccessLock.Lock()
	defer s.bucketAccessLock.Unlock()

	// shutdown must be called on every bucket
	eg := enterrors.NewErrorGroupWrapper(s.logger)
	eg.SetLimit(runtime.GOMAXPROCS(0))

	for name, bucket := range s.bucketsByName {
		name := name
		bucket := bucket

		eg.Go(func() error {
			if err := bucket.Shutdown(ctx); err != nil {
				return errors.Wrapf(err, "shutdown bucket %q of store %q", name, s.dir)
			}
			return nil
		})
	}

	return eg.Wait()
}

func (s *Store) ShutdownBucket(ctx context.Context, bucketName string) error {
	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	s.bucketAccessLock.Lock()
	defer s.bucketAccessLock.Unlock()

	bucket, ok := s.bucketsByName[bucketName]
	if !ok {
		return fmt.Errorf("shutdown bucket %q of store %q: bucket not found", bucketName, s.dir)
	}
	if err := bucket.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "shutdown bucket %q of store %q", bucketName, s.dir)
	}
	delete(s.bucketsByName, bucketName)

	return nil
}

func (s *Store) WriteWALs() error {
	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	if s.closed {
		return fmt.Errorf("%w: writing wals of store %q", ErrAlreadyClosed, s.dir)
	}

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
		basePath, err := filepath.Rel(basePath, b.GetDir())
		if err != nil {
			return nil, fmt.Errorf("bucket relative path: %w", err)
		}
		return b.ListFiles(ctx, basePath)
	}

	result, err := s.runJobOnBuckets(ctx, listFiles, nil)
	if err != nil {
		return nil, err
	}

	migrationFiles, err := s.listMigrationFiles(basePath)
	if err != nil {
		return nil, err
	}

	files := migrationFiles
	for _, res := range result {
		files = append(files, res.([]string)...)
	}

	return files, nil
}

func (s *Store) listMigrationFiles(basePath string) ([]string, error) {
	migrationRoot := filepath.Join(s.dir, ".migrations")

	var files []string
	err := filepath.WalkDir(migrationRoot, func(path string, d os.DirEntry, _ error) error {
		if d == nil || d.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(basePath, path)
		if err != nil {
			return err
		}
		files = append(files, relPath)
		return nil
	})
	if err != nil {
		return nil, errors.Errorf("failed to list files for migrations: %s", err)
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
	s.bucketAccessLock.Lock()
	var (
		status      = newBucketJobStatus()
		resultQueue = make(chan interface{}, len(s.bucketsByName))
		wg          = sync.WaitGroup{}
	)

	for _, bucket := range s.bucketsByName {
		wg.Add(1)
		b := bucket
		f := func() {
			status.Lock()
			defer status.Unlock()
			res, err := jobFunc(ctx, b)
			resultQueue <- res
			status.buckets[b] = err
			wg.Done()
		}
		enterrors.GoWrapper(f, s.logger)
	}
	s.bucketAccessLock.Unlock()
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
	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	if s.closed {
		return fmt.Errorf("%w: adding a bucket %q to store %q", ErrAlreadyClosed, bucketName, s.dir)
	}

	s.bucketsLocks.Lock(bucketName)
	defer s.bucketsLocks.Unlock(bucketName)

	if b := s.Bucket(bucketName); b != nil {
		return fmt.Errorf("bucket %s exists and is already in use", bucketName)
	}

	bucketDir := s.bucketDir(bucketName)
	if err := os.RemoveAll(bucketDir); err != nil {
		return errors.Wrapf(err, "failed removing bucket %s files", bucketName)
	}

	compactionCallbacks := s.cycleCallbacks.compactionCallbacks
	if bucketName == helpers.ObjectsBucketLSM && s.cycleCallbacks.compactionAuxCallbacks != nil {
		compactionCallbacks = s.cycleCallbacks.compactionAuxCallbacks
	}

	b, err := s.bcreator.NewBucket(ctx, bucketDir, s.rootDir, s.logger, s.metrics,
		compactionCallbacks, s.cycleCallbacks.flushCallbacks, opts...)
	if err != nil {
		return err
	}

	s.setBucket(bucketName, b)

	return nil
}

func (s *Store) replaceBucket(ctx context.Context, replacementBucket *Bucket, replacementBucketName string, bucket *Bucket, bucketName string) (string, string, string, string, error) {
	replacementBucket.disk.maintenanceLock.Lock()
	defer replacementBucket.disk.maintenanceLock.Unlock()

	currBucketDir := bucket.dir
	newBucketDir := bucket.dir + "___del"
	currReplacementBucketDir := replacementBucket.dir
	newReplacementBucketDir := currBucketDir

	if err := bucket.Shutdown(ctx); err != nil {
		return "", "", "", "", errors.Wrapf(err, "failed shutting down bucket old '%s'", bucketName)
	}

	s.logger.WithField("action", "lsm_replace_bucket").
		WithField("bucket", bucketName).
		WithField("replacement_bucket", replacementBucketName).
		WithField("dir", s.dir).
		Info("replacing bucket")

	replacementBucket.flushLock.Lock()
	defer replacementBucket.flushLock.Unlock()
	if err := os.Rename(currBucketDir, newBucketDir); err != nil {
		return "", "", "", "", errors.Wrapf(err, "failed moving orig bucket dir '%s'", currBucketDir)
	}
	if err := os.Rename(currReplacementBucketDir, newReplacementBucketDir); err != nil {
		return "", "", "", "", errors.Wrapf(err, "failed moving replacement bucket dir '%s'", currReplacementBucketDir)
	}

	return currBucketDir, newBucketDir, currReplacementBucketDir, newReplacementBucketDir, nil
}

// Replaces 1st bucket with 2nd one. Both buckets have to registered in bucketsByName.
// 2nd bucket swaps the 1st one in bucketsByName using 1st one's name, 2nd one's name is deleted.
// Dir path of 2nd bucket is changed to dir of 1st bucket as well as all other related paths of
// bucket resources (segment group, memtables, commit log).
// Dir path of 1st bucket is temporarily suffixed with "___del", later on bucket is shutdown and
// its files deleted.
// 2nd bucket becomes 1st bucket
func (s *Store) ReplaceBuckets(ctx context.Context, bucketName, replacementBucketName string) error {
	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	if s.closed {
		return fmt.Errorf("%w: replacing bucket %q for %q in store %q", ErrAlreadyClosed, bucketName, replacementBucketName, s.dir)
	}

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

	var currBucketDir, newBucketDir, currReplacementBucketDir, newReplacementBucketDir string
	var err error
	currBucketDir, newBucketDir, currReplacementBucketDir, newReplacementBucketDir, err = s.replaceBucket(ctx, replacementBucket, replacementBucketName, bucket, bucketName)
	if err != nil {
		return errors.Wrapf(err, "failed renaming bucket '%s' to '%s'", bucketName, replacementBucketName)
	}

	replacementBucket.flushLock.Lock()
	defer replacementBucket.flushLock.Unlock()

	if replacementBucket.flushing != nil {
		return fmt.Errorf("bucket '%s' can not be renamed before flushing", replacementBucketName)
	}

	replacementBucket.dir = newReplacementBucketDir

	err = replacementBucket.setNewActiveMemtable()
	if err != nil {
		return fmt.Errorf("switch active memtable: %w", err)
	}

	s.updateBucketDir(bucket, currBucketDir, newBucketDir)
	s.updateBucketDir(replacementBucket, currReplacementBucketDir, newReplacementBucketDir)

	if err := os.RemoveAll(newBucketDir); err != nil {
		return errors.Wrapf(err, "failed removing dir '%s'", newBucketDir)
	}

	return nil
}

func (s *Store) RenameBucket(ctx context.Context, bucketName, newBucketName string) error {
	s.closeLock.RLock()
	defer s.closeLock.RUnlock()

	if s.closed {
		return fmt.Errorf("%w: renaming bucket %q for %q in store %q", ErrAlreadyClosed, bucketName, newBucketName, s.dir)
	}

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

	if !currBucket.isReadOnly() {
		return fmt.Errorf("bucket '%s' must be in %s mode to be renamed", bucketName, storagestate.StatusReadOnly)
	}

	currBucketDir := currBucket.dir
	newBucketDir := s.bucketDir(newBucketName)

	currBucket.flushLock.Lock()
	defer currBucket.flushLock.Unlock()

	if currBucket.flushing != nil {
		return fmt.Errorf("bucket '%s' can not be renamed before flushing", bucketName)
	}

	currBucket.dir = newBucketDir

	err := currBucket.setNewActiveMemtable()
	if err != nil {
		return fmt.Errorf("switch active memtable: %w", err)
	}

	s.bucketsByName[newBucketName] = currBucket
	delete(s.bucketsByName, bucketName)

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

	segments, release := bucket.disk.getAndLockSegments()
	defer release()

	bucket.disk.dir = newBucketDir
	for _, segment := range segments {
		segment.setPath(updatePath(segment.getPath()))
	}
}
