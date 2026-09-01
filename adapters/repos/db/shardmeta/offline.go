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

package shardmeta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// offlineOpenTimeout bounds the wait for the file lock in the *Offline
// helpers. Only a loaded shard holds the lock, and these helpers address
// unloaded shards, so waiting is a sign the caller raced a load rather than
// something to sit out.
const offlineOpenTimeout = time.Second

// GetOffline reads key from ns of an UNLOADED shard's metadata DB, opening
// the file read-only for the duration of the call and never creating it.
// ok=false with a nil error means the file does not exist — state positively
// known to be absent. A locked (a loaded shard owns it) or unreadable file
// returns an error, so callers can tell "no state" apart from "state we
// failed to read".
func GetOffline(shardDir, ns string, key []byte) (val []byte, ok bool, err error) {
	path := filepath.Join(shardDir, FileName)
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: offlineOpenTimeout})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open shard metadata db %q: %w", path, err)
	}
	defer db.Close()

	if err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(ns))
		if b == nil {
			return nil
		}
		if v := b.Get(key); v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	}); err != nil {
		return nil, false, fmt.Errorf("read shard metadata namespace %q: %w", ns, err)
	}
	return val, true, nil
}

// DeleteOffline removes key from ns of an UNLOADED shard's metadata DB,
// opening the file briefly and never creating it (bbolt.Open CREATES missing
// files, so the path is statted first). A missing file, a missing namespace,
// or a file locked by a loaded shard is success: nothing was recorded, or
// the loaded owner deletes through its own handle.
func DeleteOffline(shardDir, ns string, key []byte) error {
	path := filepath.Join(shardDir, FileName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat shard metadata db: %w", err)
	}

	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: offlineOpenTimeout})
	if err != nil {
		if errors.Is(err, bolterrors.ErrTimeout) {
			return nil
		}
		return fmt.Errorf("open shard metadata db: %w", err)
	}
	defer db.Close()

	if err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(ns))
		if b == nil {
			return nil
		}
		return b.Delete(key)
	}); err != nil {
		return fmt.Errorf("delete %q from shard metadata namespace %q: %w", key, ns, err)
	}
	return nil
}
