package modulestorage

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/sirupsen/logrus"
)

type Repo struct {
	logger  logrus.FieldLogger
	baseDir string
	db      *bolt.DB
}

func NewRepo(baseDir string, logger logrus.FieldLogger) (*Repo, error) {
	r := &Repo{
		baseDir: baseDir,
		logger:  logger,
	}

	err := r.init()
	return r, err
}

func (r *Repo) DBPath() string {
	return fmt.Sprintf("%s/modules.db", r.baseDir)
}

func (r *Repo) init() error {
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

func (r *Repo) Storage(bucketName string) (modules.Storage, error) {
	storage := &storageBucket{
		bucketKey: []byte(bucketName),
		repo:      r,
	}

	err := storage.init()
	return storage, err
}

func (s *storageBucket) init() error {
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
			return fmt.Errorf("no bucket for key %s found", string(s.bucketKey))
		}

		if err := b.Put(key, value); err != nil {
			return errors.Wrapf(err, "put value for key %s", string(key))
		}

		return nil
	})
}

func (s *storageBucket) Get(key []byte) ([]byte, error) {
	var out []byte
	err := s.repo.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketKey)
		if b == nil {
			return fmt.Errorf("no bucket for key %s found", string(s.bucketKey))
		}

		out = b.Get(key)
		return nil
	})

	return out, err
}
