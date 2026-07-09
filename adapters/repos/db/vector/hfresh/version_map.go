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

package hfresh

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
)

var ErrVersionIncrementFailed = errors.New("version increment failed")

// A VectorVersion is a 1-byte value structured as follows:
// - 7 bits for the version number
// - 1 bit for the tombstone flag (0 = alive, 1 = deleted)
// TODO: versions can wrap around after 127 updates,
// we need a mechanism to handle this in the future (e.g. during snapshots perhaps, etc.)
type VectorVersion uint8

func (ve VectorVersion) Version() uint8 {
	return uint8(ve) & counterMask
}

func (ve VectorVersion) Deleted() bool {
	return (uint8(ve) & tombstoneMask) != 0
}

func (ve VectorVersion) Increment() VectorVersion {
	delBit := uint8(ve) & tombstoneMask // 0x00 or 0x80
	counter := uint8(ve) & counterMask  // 0-127

	if counter < 127 {
		counter++
	} else {
		counter = 0 // wraparound behavior
	}

	return VectorVersion(delBit | counter)
}

var v1 = VectorVersion(0).Increment()

// VersionMap keeps track of the version of each vector.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type VersionMap struct {
	data  *common.GroupedPagedArray[VectorVersion]
	locks *common.ShardedRWLocks
	store *VersionStore
}

func NewVersionMap(bucket *lsmkv.Bucket) *VersionMap {
	return &VersionMap{
		data:  common.NewGroupedPagedArray[VectorVersion](16*1024, 64*1024), // 1 billion entries with 64k per page
		locks: common.NewShardedRWLocks(512),
		store: NewVersionStore(bucket),
	}
}

// Get returns the size of the vector with the given ID.
func (v *VersionMap) Get(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	page, slot := v.data.GetPageFor(vectorID)
	if page == nil {
		// not in cache, check store
		version, err := v.store.Get(ctx, vectorID)
		if err != nil && !errors.Is(err, ErrVectorNotFound) {
			return 0, errors.Wrapf(err, "failed to get version for vector %d", vectorID)
		}
		if errors.Is(err, ErrVectorNotFound) {
			version = v1
		}

		// update cache
		page, slot := v.data.EnsurePageFor(vectorID)
		v.locks.Lock(vectorID)
		page[slot] = version
		v.locks.Unlock(vectorID)

		return version, nil
	}

	v.locks.RLock(vectorID)
	version := page[slot]
	v.locks.RUnlock(vectorID)

	if version == 0 {
		v.locks.Lock(vectorID)
		defer v.locks.Unlock(vectorID)

		// double-check after acquiring the lock
		version = page[slot]
		if version != 0 {
			return version, nil
		}

		// not in cache, check store
		var err error
		version, err = v.store.Get(ctx, vectorID)
		if err != nil && !errors.Is(err, ErrVectorNotFound) {
			return 0, errors.Wrapf(err, "failed to get version for vector %d", vectorID)
		}
		if errors.Is(err, ErrVectorNotFound) {
			version = v1
		}

		// update cache
		page[slot] = version
	}

	return version, nil
}

// Incr increments the version of the vector and returns the new version.
func (v *VersionMap) Increment(ctx context.Context, vectorID uint64, previousVersion VectorVersion) (VectorVersion, error) {
	var err error

	page, slot := v.data.EnsurePageFor(vectorID)
	v.locks.Lock(vectorID)
	defer v.locks.Unlock(vectorID)

	old := page[slot]
	if old == 0 {
		// not in cache, check store
		old, err = v.store.Get(ctx, vectorID)
		if err != nil && !errors.Is(err, ErrVectorNotFound) {
			return 0, errors.Wrapf(err, "failed to get version for vector %d", vectorID)
		}
		if errors.Is(err, ErrVectorNotFound) {
			old = v1
		}
	}

	if old.Deleted() || old != previousVersion {
		return old, ErrVersionIncrementFailed
	}

	newVersion := old.Increment()
	err = v.store.Set(ctx, vectorID, newVersion)
	if err != nil {
		return old, err
	}
	page[slot] = newVersion
	return newVersion, nil
}

func (v *VersionMap) MarkDeleted(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	var err error

	page, slot := v.data.EnsurePageFor(vectorID)
	v.locks.Lock(vectorID)
	defer v.locks.Unlock(vectorID)

	old := page[slot]
	if old == 0 {
		// not in cache, check store
		old, err = v.store.Get(ctx, vectorID)
		if err != nil && !errors.Is(err, ErrVectorNotFound) {
			return 0, errors.Wrapf(err, "failed to get version for vector %d", vectorID)
		}
		if errors.Is(err, ErrVectorNotFound) {
			old = v1
		}
	}

	if old.Deleted() {
		return old, nil
	}

	counter := uint8(old) & counterMask // 0-127
	newVersion := VectorVersion(tombstoneMask | counter)
	err = v.store.Set(ctx, vectorID, newVersion)
	if err != nil {
		return old, err
	}
	page[slot] = newVersion

	return newVersion, nil
}

// defaultRestoreConcurrency bounds how many parallel cursor scans Restore
// runs. A single LSM cursor over ~1B tiny entries is CPU-bound in the
// segment heap-merge (~3M entries/s measured), so the version keyspace is
// partitioned by the first byte of the LE-encoded vector ID (256 disjoint
// ranges) and scanned in parallel. Overridable via
// HFRESH_RESTORE_CONCURRENCY (1 = sequential).
const defaultRestoreConcurrency = 16

// Restore bulk-loads every persisted vector version into the in-memory paged
// array and returns how many were loaded. It must run at startup, before the
// index serves queries: the map is otherwise populated lazily, and on a
// freshly started node every first access of a vector ID during the posting
// scan falls back to an LSM point read — thousands of random disk reads per
// query until the full ID space has been touched.
//
// The paged array handles concurrent page installation; each ID belongs to
// exactly one partition, so slot writes are disjoint across workers. Callers
// must not use the map concurrently until Restore returns.
func (v *VersionMap) Restore(ctx context.Context, logger logrus.FieldLogger) (int64, error) {
	concurrency := envIntOrDefault("HFRESH_RESTORE_CONCURRENCY", defaultRestoreConcurrency)

	counts := make([]int64, concurrency)

	eg := enterrors.NewErrorGroupWrapper(logger)
	for w := range concurrency {
		eg.Go(func() error {
			for b := w; b < 256; b += concurrency {
				err := v.store.iterFirstByte(ctx, byte(b), func(vectorID uint64, version VectorVersion) error {
					if version == 0 {
						return nil
					}
					page, slot := v.data.EnsurePageFor(vectorID)
					page[slot] = version
					counts[w]++
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, err
	}

	var count int64
	for _, c := range counts {
		count += c
	}
	return count, nil
}

func (v *VersionMap) IsDeleted(ctx context.Context, vectorID uint64) (bool, error) {
	version, err := v.Get(ctx, vectorID)
	if err != nil {
		return false, err
	}
	return version.Deleted(), nil
}

// VersionStore is a persistent store for vector versions.
// It stores the versions in an LSMKV bucket.
type VersionStore struct {
	bucket *lsmkv.Bucket
}

func NewVersionStore(bucket *lsmkv.Bucket) *VersionStore {
	return &VersionStore{
		bucket: bucket,
	}
}

func (v *VersionStore) key(vectorID uint64) []byte {
	buf := make([]byte, len(versionMapBucketPrefix)+8)
	copy(buf, versionMapBucketPrefix)
	binary.LittleEndian.PutUint64(buf[len(versionMapBucketPrefix):], vectorID)
	return buf
}

func (v *VersionStore) Get(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	key := v.key(vectorID)
	version, err := v.bucket.Get(key[:])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get version for %d", vectorID)
	}

	if len(version) == 0 {
		return 0, ErrVectorNotFound
	}

	return VectorVersion(version[0]), nil
}

func (v *VersionStore) Set(ctx context.Context, vectorID uint64, version VectorVersion) error {
	key := v.key(vectorID)
	return v.bucket.Put(key[:], []byte{byte(version)})
}

// iterFirstByte calls fn for every persisted vector version whose LE-encoded
// ID starts with firstByte — one of the 256 disjoint partitions of the
// version keyspace.
func (v *VersionStore) iterFirstByte(ctx context.Context, firstByte byte, fn func(uint64, VectorVersion) error) error {
	c := v.bucket.Cursor()
	defer c.Close()

	seek := make([]byte, len(versionMapBucketPrefix)+1)
	copy(seek, versionMapBucketPrefix)
	seek[len(versionMapBucketPrefix)] = firstByte

	var i int
	for k, val := c.Seek(seek); len(k) > len(versionMapBucketPrefix) &&
		bytes.HasPrefix(k, versionMapBucketPrefix) &&
		k[len(versionMapBucketPrefix)] == firstByte; k, val = c.Next() {
		i++
		if len(val) == 0 {
			continue
		}

		if i%1000 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}

		vectorID := binary.LittleEndian.Uint64(k[len(versionMapBucketPrefix):])
		if err := fn(vectorID, VectorVersion(val[0])); err != nil {
			return err
		}
	}

	return ctx.Err()
}
