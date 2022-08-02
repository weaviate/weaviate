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
