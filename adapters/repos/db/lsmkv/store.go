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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Store groups multiple buckets together, it "owns" one folder on the file
// system
type Store struct {
	rootDir       string
	bucketsByName map[string]*Bucket
	logger        logrus.FieldLogger
}

func New(rootDir string, logger logrus.FieldLogger) (*Store, error) {
	s := &Store{
		rootDir:       rootDir,
		bucketsByName: map[string]*Bucket{},
		logger:        logger,
	}

	return s, s.init()
}

func (s *Store) Bucket(name string) *Bucket {
	return s.bucketsByName[name]
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
	opts ...BucketOption) error {
	if _, ok := s.bucketsByName[bucketName]; ok {
		return nil
	}

	b, err := NewBucket(ctx, s.bucketDir(bucketName), s.logger, opts...)
	if err != nil {
		return err
	}

	s.bucketsByName[bucketName] = b
	return nil
}

func (s *Store) Shutdown(ctx context.Context) error {
	for name, bucket := range s.bucketsByName {
		if err := bucket.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shtudown bucket %q", name)
		}
	}

	return nil
}

func (s *Store) WriteWALs() error {
	for name, bucket := range s.bucketsByName {
		if err := bucket.WriteWAL(); err != nil {
			return errors.Wrapf(err, "bucket %q", name)
		}
	}

	return nil
}
