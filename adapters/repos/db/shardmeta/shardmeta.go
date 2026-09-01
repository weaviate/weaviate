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

// Package shardmeta owns the shard-level metadata database (<shard>/index.db).
// The shard opens and closes it, serializes updates, takes consistent
// snapshots for backups, and its file is removed only together with the whole
// shard directory. Consumers (today: the dynamic vector index's flat-to-hnsw
// state) receive bounded, namespace-scoped operations and never touch the
// file or the bolt handle themselves.
package shardmeta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
)

// FileName is the metadata DB's name inside the shard directory. It aliases
// the historical constant: the file began life as the dynamic index's state
// DB and existing deployments already have it on disk under that name. The
// constant itself stays in entities so entities/backup can keep referencing
// it without importing adapters.
const FileName = ent.StateDBFileName

// DB is one shard's metadata database.
type DB struct {
	db   *bbolt.DB
	path string
}

// Open opens shardDir's metadata DB, creating the file if it does not exist.
// timeout bounds the wait for the file lock: a leaked handle from a failed
// shard teardown holds the flock, and without a timeout bolt retries forever
// and wedges the loading goroutine.
func Open(shardDir string, timeout time.Duration) (*DB, error) {
	path := filepath.Join(shardDir, FileName)
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("open shard metadata db %q: %w", path, err)
	}
	return &DB{db: db, path: path}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

// IsClosed reports whether err came from an operation on a DB whose handle
// was already closed. Teardown paths that race shutdown (a drop on a shard
// that was shut down first) use it to treat "nothing left to update" as done.
func IsClosed(err error) bool {
	return errors.Is(err, bolterrors.ErrDatabaseNotOpen)
}

// Snapshot writes a consistent point-in-time copy of the DB into stagingDir,
// preserving the DB's path relative to basePath, and returns that relative
// path. The copy is taken inside a bolt read transaction (tx.CopyFile) so an
// in-place write during a long upload window cannot tear the staged copy.
func (d *DB) Snapshot(basePath, stagingDir string) (string, error) {
	relPath, err := filepath.Rel(basePath, d.path)
	if err != nil {
		return "", fmt.Errorf("compute relative path for %s: %w", FileName, err)
	}
	if !filepath.IsLocal(relPath) {
		return "", fmt.Errorf("shard metadata db %q is outside backup base %q", d.path, basePath)
	}
	dst := filepath.Join(stagingDir, relPath)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", fmt.Errorf("create staging subdir for %s: %w", relPath, err)
	}
	if err := d.db.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(dst, 0o600)
	}); err != nil {
		return "", fmt.Errorf("snapshot %s to staging: %w", FileName, err)
	}
	return relPath, nil
}

// Namespace returns bounded operations scoped to one top-level bucket.
func (d *DB) Namespace(name string) *Namespace {
	return &Namespace{db: d.db, name: []byte(name)}
}

// Namespace is bounded access to one top-level bucket of a shard's metadata
// DB. It cannot reach other namespaces, close the DB, or touch the file.
type Namespace struct {
	db   *bbolt.DB
	name []byte
}

// Get returns a copy of key's value, or nil when the key or the whole
// namespace has never been written.
func (n *Namespace) Get(key []byte) ([]byte, error) {
	var val []byte
	err := n.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(n.name)
		if b == nil {
			return nil
		}
		if v := b.Get(key); v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("get %q from shard metadata namespace %q: %w", key, n.name, err)
	}
	return val, nil
}

// Put stores value under key, creating the namespace on first write.
func (n *Namespace) Put(key, value []byte) error {
	err := n.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(n.name)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
	if err != nil {
		return fmt.Errorf("put %q into shard metadata namespace %q: %w", key, n.name, err)
	}
	return nil
}

// Delete removes key. A namespace or key that was never written is already
// deleted.
func (n *Namespace) Delete(key []byte) error {
	err := n.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(n.name)
		if b == nil {
			return nil
		}
		return b.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("delete %q from shard metadata namespace %q: %w", key, n.name, err)
	}
	return nil
}
