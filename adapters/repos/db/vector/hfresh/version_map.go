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
//
// Locking is two-tier so that readers never wait on disk:
//   - locks guards the in-memory pages only; every hold is nanoseconds.
//     Search scans take it read-side for every candidate, so no LSM
//     operation is ever performed while holding it.
//   - persistLocks serializes store writes per vector among writers. Each
//     persist re-reads the current memory value under the read lock, so the
//     last persist among racing writers always lands the newest value and
//     memory and store converge.
type VersionMap struct {
	data         *common.GroupedPagedArray[VectorVersion]
	locks        *common.ShardedRWLocks
	persistLocks *common.ShardedLocks
	store        *VersionStore
}

func NewVersionMap(bucket *lsmkv.Bucket) *VersionMap {
	return &VersionMap{
		data:         common.NewGroupedPagedArray[VectorVersion](16*1024, 64*1024), // 1 billion entries with 64k per page
		locks:        common.NewShardedRWLocks(512),
		persistLocks: common.NewShardedLocks(512),
		store:        NewVersionStore(bucket),
	}
}

// Get returns the version of the vector with the given ID.
func (v *VersionMap) Get(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	page, slot := v.data.GetPageFor(vectorID)
	if page != nil {
		v.locks.RLock(vectorID)
		version := page[slot]
		v.locks.RUnlock(vectorID)
		if version != 0 {
			return version, nil
		}
	}

	return v.loadInto(ctx, vectorID)
}

// loadInto fetches the version from the store — without holding any lock, so
// concurrent readers never queue behind an LSM read — and installs it into
// memory only if no writer beat us to it. The memory value always wins: it is
// at least as new as anything the store returned before we took the lock.
func (v *VersionMap) loadInto(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	loaded, err := v.store.Get(ctx, vectorID)
	if err != nil && !errors.Is(err, ErrVectorNotFound) {
		return 0, errors.Wrapf(err, "failed to get version for vector %d", vectorID)
	}
	if errors.Is(err, ErrVectorNotFound) {
		loaded = v1
	}

	page, slot := v.data.EnsurePageFor(vectorID)
	v.locks.Lock(vectorID)
	if page[slot] == 0 {
		page[slot] = loaded
	}
	version := page[slot]
	v.locks.Unlock(vectorID)

	return version, nil
}

// persist writes the current in-memory version of the vector to the store.
// Persists are serialized per vector, and each re-reads memory after
// acquiring the persist lock, so the last persist among racing writers lands
// the newest value. The memory lock is never held across the LSM write.
func (v *VersionMap) persist(ctx context.Context, vectorID uint64, page []VectorVersion, slot int) error {
	v.persistLocks.Lock(vectorID)
	defer v.persistLocks.Unlock(vectorID)

	v.locks.RLock(vectorID)
	cur := page[slot]
	v.locks.RUnlock(vectorID)

	return v.store.Set(ctx, vectorID, cur)
}

// Increment increments the version of the vector and returns the new version.
// It fails with ErrVersionIncrementFailed if the current version does not
// match previousVersion or the vector is deleted. On a persist error the
// memory update has already happened; a retry (Get + Increment) re-persists
// and heals the store.
func (v *VersionMap) Increment(ctx context.Context, vectorID uint64, previousVersion VectorVersion) (VectorVersion, error) {
	// ensure the entry is loaded so the CAS below never touches the store
	if _, err := v.Get(ctx, vectorID); err != nil {
		return 0, err
	}

	page, slot := v.data.GetPageFor(vectorID)
	v.locks.Lock(vectorID)
	old := page[slot]
	if old.Deleted() || old != previousVersion {
		v.locks.Unlock(vectorID)
		return old, ErrVersionIncrementFailed
	}
	newVersion := old.Increment()
	page[slot] = newVersion
	v.locks.Unlock(vectorID)

	if err := v.persist(ctx, vectorID, page, slot); err != nil {
		return newVersion, err
	}
	return newVersion, nil
}

// MarkDeleted sets the tombstone bit for the vector. It always persists, even
// when the vector is already deleted in memory, so a retry after a failed
// persist heals a store that missed the tombstone.
func (v *VersionMap) MarkDeleted(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	// ensure the entry is loaded so the update below never touches the store
	if _, err := v.Get(ctx, vectorID); err != nil {
		return 0, err
	}

	page, slot := v.data.GetPageFor(vectorID)
	v.locks.Lock(vectorID)
	old := page[slot]
	newVersion := old
	if !old.Deleted() {
		counter := uint8(old) & counterMask // 0-127
		newVersion = VectorVersion(tombstoneMask | counter)
		page[slot] = newVersion
	}
	v.locks.Unlock(vectorID)

	if err := v.persist(ctx, vectorID, page, slot); err != nil {
		return newVersion, err
	}

	return newVersion, nil
}

func (v *VersionMap) IsDeleted(ctx context.Context, vectorID uint64) (bool, error) {
	version, err := v.Get(ctx, vectorID)
	if err != nil {
		return false, err
	}
	return version.Deleted(), nil
}

// FlushPosting ensures that all vectors are persisted with at least version v1.
// This is used during analyze to ensure that all vectors have a version on-disk, even if they haven't been updated since they were added to the posting.
// This is necessary because the in-memory version map only tracks versions for vectors
// that have been reassigned or deleted, for performance reasons.
func (v *VersionMap) FlushPosting(ctx context.Context, postingID uint64, p Posting) error {
	for _, vector := range p {
		curVer, err := v.Get(ctx, vector.ID())
		if err != nil {
			return errors.Wrap(err, "failed to get current version for vector during flush")
		}
		if curVer > v1 || curVer.Deleted() {
			// only flush versions for vectors that are still at the initial version,
			// the other versions are already persisted by Increment or MarkDeleted operations
			continue
		}

		// check if the version already exists on-disk to avoid unnecessary writes
		_, err = v.store.Get(ctx, vector.ID())
		if err != nil && !errors.Is(err, ErrVectorNotFound) {
			return errors.Wrap(err, "failed to get version for vector during flush")
		}
		if err == nil {
			// version already exists on-disk, no need to flush
			continue
		}
		// version does not exist on-disk, flush the initial version
		err = v.store.Set(ctx, vector.ID(), v1)
		if err != nil {
			return errors.Wrap(err, "failed to set version for vector during flush")
		}
	}

	return nil
}

// VectorExists checks if a version exists for the given vector ID.
// It reads from the disk to ensure that we don't return false positives for vectors that are not in the cache but do exist on-disk.
func (v *VersionMap) VectorExists(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	ver, err := v.store.Get(ctx, vectorID)
	if err != nil {
		if errors.Is(err, ErrVectorNotFound) {
			return 0, nil
		}
		return 0, errors.Wrap(err, "failed to check if vector exists")
	}

	return ver, nil
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
