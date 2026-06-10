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

package modulestorage

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	bolt "go.etcd.io/bbolt"
)

type Repo struct {
	logger   logrus.FieldLogger
	baseDir  string
	db       *bolt.DB
	readOnly bool
}

func NewRepo(baseDir string, logger logrus.FieldLogger) (*Repo, error) {
	r := &Repo{
		baseDir: baseDir,
		logger:  logger,
	}

	err := r.init()
	return r, err
}

// NewRepoReadOnly opens the module storage for a read-only follower: the bolt
// file is opened read-only and module storage buckets are never created. Reads
// (Get/Scan) still work so a reader sees exactly the writer's module state (e.g.
// contextionary extensions that affect vectorization); writes are rejected by
// the read-only bolt and blocked at the API layer.
func NewRepoReadOnly(baseDir string, logger logrus.FieldLogger) (*Repo, error) {
	r := &Repo{
		baseDir:  baseDir,
		logger:   logger,
		readOnly: true,
	}

	err := r.init()
	return r, err
}

func (r *Repo) DBPath() string {
	return fmt.Sprintf("%s/modules.db", r.baseDir)
}

func (r *Repo) DataPath() string {
	return r.baseDir
}

func (r *Repo) init() error {
	if r.readOnly {
		// Read-only follower: the directory exists in the copy and the bolt file
		// must be opened read-only. Skip MkdirAll and open with ReadOnly so no
		// meta page is written.
		boltdb, err := bolt.Open(r.DBPath(), 0o600, &bolt.Options{ReadOnly: true})
		if err != nil {
			return errors.Wrapf(err, "open bolt read-only at %s", r.DBPath())
		}
		r.db = boltdb
		return nil
	}

	if err := os.MkdirAll(r.baseDir, 0o777); err != nil {
		return errors.Wrapf(err, "create root path directory at %s", r.baseDir)
	}

	boltdb, err := bolt.Open(r.DBPath(), 0o600, nil)
	if err != nil {
		return errors.Wrapf(err, "open bolt at %s", r.DBPath())
	}

	r.db = boltdb

	return nil
}

type storageBucket struct {
	bucketKey []byte
	repo      *Repo
}

func (r *Repo) Storage(bucketName string) (moduletools.Storage, error) {
	storage := &storageBucket{
		bucketKey: []byte(bucketName),
		repo:      r,
	}

	err := storage.init()
	return storage, err
}

func (s *storageBucket) init() error {
	if s.repo.readOnly {
		// Read-only follower: never create the bucket (the bolt is read-only).
		// If the bucket is absent in the copy, Get/Scan tolerate it (nil-checked)
		// and return empty.
		return nil
	}
	return s.repo.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(s.bucketKey); err != nil {
			return errors.Wrapf(err, "create module storage bucket '%s'",
				string(s.bucketKey))
		}

		return nil
	})
}

func (s *storageBucket) Put(key, value []byte) error {
	return s.repo.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketKey)
		if b == nil {
			return errors.Errorf("no bucket for key %s found", string(s.bucketKey))
		}

		if err := b.Put(key, value); err != nil {
			return errors.Wrapf(err, "put value for key %s", string(key))
		}

		return nil
	})
}

func (s *storageBucket) Get(key []byte) ([]byte, error) {
	var out []byte
	err := s.repo.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketKey)
		if b == nil {
			return errors.Errorf("no bucket for key %s found", string(s.bucketKey))
		}

		out = b.Get(key)
		return nil
	})

	return out, err
}

func (s *storageBucket) Scan(scan moduletools.ScanFn) error {
	err := s.repo.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketKey)
		if b == nil {
			// No bucket for this key — treat as an empty scan, consistent with
			// Get/Put which both nil-check. Without this guard b.Cursor() panics
			// on a nil bucket, which a read-only-aware storage (that skips bucket
			// creation) would otherwise hit.
			return nil
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			ok, err := scan(k, v)
			if err != nil {
				return errors.Wrapf(err, "read item %q", string(k))
			}

			if !ok {
				break
			}
		}

		return nil
	})

	return err
}
