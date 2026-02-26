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
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
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
