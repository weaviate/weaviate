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

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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
	cache *otter.Cache[uint64, VectorVersion]
	store *VersionStore
}

func NewVersionMap(bucket *lsmkv.Bucket) *VersionMap {
	cache, _ := otter.New[uint64, VectorVersion](nil)

	return &VersionMap{
		cache: cache,
		store: NewVersionStore(bucket),
	}
}

// Get returns the size of the vector with the given ID.
func (v *VersionMap) Get(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	loader := otter.LoaderFunc[uint64, VectorVersion](func(ctx context.Context, key uint64) (VectorVersion, error) {
		version, err := v.store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrVectorNotFound) {
				return v1, nil
			}

			return 0, err
		}

		return version, nil
	})
	version, err := v.cache.Get(ctx, vectorID, loader)
	if errors.Is(err, otter.ErrNotFound) {
		return 0, ErrVectorNotFound
	}

	return version, err
}

// Incr increments the version of the vector and returns the new version.
func (v *VersionMap) Increment(ctx context.Context, vectorID uint64, previousVersion VectorVersion) (VectorVersion, error) {
	var err error
	version, _ := v.cache.Compute(vectorID, func(oldVersion VectorVersion, found bool) (newValue VectorVersion, op otter.ComputeOp) {
		if !found {
			oldVersion, err = v.store.Get(ctx, vectorID)
			if err != nil && !errors.Is(err, ErrVectorNotFound) {
				return 0, otter.CancelOp
			}
			if err != nil {
				oldVersion = v1
			}
		}
		if oldVersion.Deleted() || oldVersion != previousVersion {
			err = ErrVersionIncrementFailed
			return oldVersion, otter.CancelOp
		}

		newVersion := oldVersion.Increment()
		err = v.store.Set(ctx, vectorID, newVersion)
		if err != nil {
			return oldVersion, otter.CancelOp
		}

		return newVersion, otter.WriteOp
	})
	return version, err
}

func (v *VersionMap) MarkDeleted(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	var err error
	version, _ := v.cache.Compute(vectorID, func(oldVersion VectorVersion, found bool) (newValue VectorVersion, op otter.ComputeOp) {
		if !found {
			oldVersion, err = v.store.Get(ctx, vectorID)
			if err != nil && !errors.Is(err, ErrVectorNotFound) {
				return 0, otter.CancelOp
			}
			if err != nil {
				oldVersion = v1
			}
		}

		if oldVersion.Deleted() {
			return oldVersion, otter.CancelOp
		}

		counter := uint8(oldVersion) & counterMask // 0-127
		newVersion := VectorVersion(tombstoneMask | counter)
		err = v.store.Set(ctx, vectorID, newVersion)
		if err != nil {
			return oldVersion, otter.CancelOp
		}

		return VectorVersion(newVersion), otter.WriteOp
	})
	return version, err
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
func (v *VersionMap) VectorExists(ctx context.Context, vectorID uint64) (bool, error) {
	_, err := v.store.Get(ctx, vectorID)
	if err != nil {
		if errors.Is(err, ErrVectorNotFound) {
			return false, nil
		}
		return false, errors.Wrap(err, "failed to check if vector exists")
	}

	return true, nil
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

func (v *VersionStore) key(vectorID uint64) [9]byte {
	var buf [9]byte
	buf[0] = versionMapBucketPrefix
	binary.LittleEndian.PutUint64(buf[1:], vectorID)
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
