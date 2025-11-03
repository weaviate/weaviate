//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

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

// VersionMap keeps track of the version of each vector.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type VersionMap struct {
	cache *otter.Cache[uint64, VectorVersion]
	store *VersionStore
}

func NewVersionMap(store *lsmkv.Store, id string, cfg StoreConfig) (*VersionMap, error) {
	vStore, err := NewVersionStore(store, versionBucketName(id), cfg)
	if err != nil {
		return nil, err
	}

	cache, err := otter.New(&otter.Options[uint64, VectorVersion]{
		MaximumSize: 1_000_000, // Todo: choose a better value for it
	})
	if err != nil {
		return nil, err
	}

	return &VersionMap{
		cache: cache,
		store: vStore,
	}, nil
}

// Get returns the size of the vector with the given ID.
func (v *VersionMap) Get(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	version, err := v.cache.Get(ctx, vectorID, otter.LoaderFunc[uint64, VectorVersion](func(ctx context.Context, key uint64) (VectorVersion, error) {
		version, err := v.store.Get(ctx, vectorID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return 0, otter.ErrNotFound
			}

			return 0, err
		}

		return version, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return 0, ErrPostingNotFound
	}

	return version, err
}

// Sets the size of the posting to newSize.
// This method assumes the posting has been locked for writing by the caller.
func (v *VersionMap) Set(ctx context.Context, vectorID uint64, newVersion VectorVersion) error {
	err := v.store.Set(ctx, vectorID, newVersion)
	if err != nil {
		return err
	}
	v.cache.Set(vectorID, newVersion)
	return nil
}

// Incr increments the size of the posting by delta and returns the new size.
// This method assumes the posting has been locked for writing by the caller.
func (v *VersionMap) Increment(ctx context.Context, vectorID uint64, previousVersion VectorVersion) (VectorVersion, error) {
	version, err := v.Get(ctx, vectorID)
	if err != nil {
		if !errors.Is(err, ErrPostingNotFound) {
			return 0, err
		}
	}

	if version.Deleted() || version != previousVersion {
		return version, nil
	}

	delBit := uint8(version) & tombstoneMask // 0x00 or 0x80
	counter := uint8(version) & counterMask  // 0-127

	if counter < 127 {
		counter++
	} else {
		counter = 0 // wraparound behavior
	}

	newVersion := VectorVersion(delBit | counter)
	err = v.store.Set(ctx, vectorID, newVersion)
	if err != nil {
		return 0, err
	}
	v.cache.Set(vectorID, newVersion)

	return newVersion, nil
}

func (v *VersionMap) MarkDeleted(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	version, err := v.Get(ctx, vectorID)
	if err != nil {
		if !errors.Is(err, ErrPostingNotFound) {
			return 0, err
		}
	}
	if version == 0 {
		return 0, nil
	}

	if version.Deleted() {
		return version, nil
	}

	counter := uint8(version) & counterMask // 0-127
	newVersion := VectorVersion(tombstoneMask | counter)
	err = v.Set(ctx, vectorID, newVersion)
	if err != nil {
		return 0, err
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

// VersionStore is a persistent store for vector versions.
// It stores the versions in an LSMKV bucket.
type VersionStore struct {
	store  *lsmkv.Store
	bucket *lsmkv.Bucket
}

func NewVersionStore(store *lsmkv.Store, bucketName string, cfg StoreConfig) (*VersionStore, error) {
	err := store.CreateOrLoadBucket(context.Background(),
		bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithAllocChecker(cfg.AllocChecker),
		lsmkv.WithMinMMapSize(cfg.MinMMapSize),
		lsmkv.WithMinWalThreshold(cfg.MaxReuseWalSize),
		lsmkv.WithLazySegmentLoading(cfg.LazyLoadSegments),
		lsmkv.WithWriteSegmentInfoIntoFileName(cfg.WriteSegmentInfoIntoFileName),
		lsmkv.WithWriteMetadata(cfg.WriteMetadataFilesEnabled),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &VersionStore{
		store:  store,
		bucket: store.Bucket(bucketName),
	}, nil
}

func (v *VersionStore) Get(ctx context.Context, vectorID uint64) (VectorVersion, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], vectorID)
	version, err := v.bucket.Get(buf[:])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get version for %d", vectorID)
	}

	return VectorVersion(version[0]), nil
}

func (v *VersionStore) Set(ctx context.Context, vectorID uint64, version VectorVersion) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], vectorID)

	return v.bucket.Put(buf[:], []byte{byte(version)})
}

func versionBucketName(id string) string {
	return fmt.Sprintf("spfresh_versions_%s", id)
}
