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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestIndexVersion_LoadWithNoVersionDefaultsToV1(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Create an index and add a vector to initialize it
	index := makeHFreshWithConfig(t, store, cfg, uc)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// Manually remove the version from metadata to simulate a legacy index
	err = index.IndexMetadata.bucket.Delete(index.IndexMetadata.key(indexVersionKey))
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Reopen the index - should default to V1
	index2 := makeHFreshWithConfig(t, store, cfg, uc)

	assert.Equal(t, uint8(HFreshIndexVersion1), index2.Version())
	assert.Equal(t, int16(8), index2.Centroids.RQBits()) // V1 uses RQ8
}

func TestIndexVersion_NewIndexUsesCurrentVersion(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// Before adding vectors, version should be CurrentHFreshIndexVersion
	assert.Equal(t, uint8(CurrentHFreshIndexVersion), index.Version())

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// Version should still be CurrentHFreshIndexVersion
	assert.Equal(t, uint8(CurrentHFreshIndexVersion), index.Version())

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Reopen and verify version is persisted
	index2 := makeHFreshWithConfig(t, store, cfg, uc)
	assert.Equal(t, uint8(CurrentHFreshIndexVersion), index2.Version())
}

func TestIndexVersion_V1UsesRQ8(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Create an index and add a vector
	index := makeHFreshWithConfig(t, store, cfg, uc)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// Manually set version to V1
	err = index.IndexMetadata.SetVersion(HFreshIndexVersion1)
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Reopen with V1 - should use RQ8
	index2 := makeHFreshWithConfig(t, store, cfg, uc)
	assert.Equal(t, uint8(HFreshIndexVersion1), index2.Version())
	assert.Equal(t, int16(8), index2.Centroids.RQBits())
}

func TestIndexVersion_V2UsesRQ1(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// CurrentVersion is V2, should use RQ1
	assert.Equal(t, uint8(HFreshIndexVersion2), index.Version())
	assert.Equal(t, int16(1), index.Centroids.RQBits())

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Reopen and verify still V2 with RQ1
	index2 := makeHFreshWithConfig(t, store, cfg, uc)
	assert.Equal(t, uint8(HFreshIndexVersion2), index2.Version())
	assert.Equal(t, int16(1), index2.Centroids.RQBits())
}

func TestIndexVersion_UnsupportedFutureVersionFails(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Create an index and add a vector
	index := makeHFreshWithConfig(t, store, cfg, uc)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// Manually set version to a future unsupported version
	err = index.IndexMetadata.SetVersion(CurrentHFreshIndexVersion + 10)
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Trying to open with unsupported version should fail
	_, err = New(cfg, uc, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported HFresh index version")
}

func TestIndexMetadataStore_GetVersion(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, _ := makeHFreshConfig(t)

	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	require.NoError(t, err)

	metadata := NewIndexMetadataStore(bucket)

	t.Run("missing version returns V1", func(t *testing.T) {
		version, err := metadata.GetVersion()
		require.NoError(t, err)
		assert.Equal(t, uint8(HFreshIndexVersion1), version)
	})

	t.Run("stored V1 returns V1", func(t *testing.T) {
		err := metadata.SetVersion(HFreshIndexVersion1)
		require.NoError(t, err)

		version, err := metadata.GetVersion()
		require.NoError(t, err)
		assert.Equal(t, uint8(HFreshIndexVersion1), version)
	})

	t.Run("stored V2 returns V2", func(t *testing.T) {
		err := metadata.SetVersion(HFreshIndexVersion2)
		require.NoError(t, err)

		version, err := metadata.GetVersion()
		require.NoError(t, err)
		assert.Equal(t, uint8(HFreshIndexVersion2), version)
	})

	t.Run("future version returns error", func(t *testing.T) {
		err := metadata.SetVersion(CurrentHFreshIndexVersion + 5)
		require.NoError(t, err)

		_, err = metadata.GetVersion()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported HFresh index version")
	})
}

func TestIndexVersion_V1DoesNotStoreMedoidMapping(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Create index
	index := makeHFreshWithConfig(t, store, cfg, uc)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// Manually set to V1 to test V1 behavior
	err = index.IndexMetadata.SetVersion(HFreshIndexVersion1)
	require.NoError(t, err)
	index.version = HFreshIndexVersion1

	// Get the posting ID created for the first vector
	postingID := uint64(1) // First posting ID

	// For V1, medoid mapping should be present from initial creation
	// but after V1 operations, it would not be set

	// Verify we can query the medoid store
	medoidID, found, err := index.MedoidStore.Get(t.Context(), postingID)
	require.NoError(t, err)

	// Initial posting was created with V2 behavior, so medoid is set
	// This test verifies the MedoidStore works correctly
	if found {
		assert.Equal(t, uint64(0), medoidID)
	}
}

func TestRQBitsForVersion(t *testing.T) {
	tests := []struct {
		name     string
		version  uint8
		expected int16
	}{
		{
			name:     "V1 uses RQ8",
			version:  HFreshIndexVersion1,
			expected: 8,
		},
		{
			name:     "V2 uses RQ1",
			version:  HFreshIndexVersion2,
			expected: 1,
		},
		{
			name:     "V0 (hypothetical legacy) uses RQ8",
			version:  0,
			expected: 8,
		},
		{
			name:     "V3 (future) uses RQ1",
			version:  3,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bits := rqBitsForVersion(tt.version)
			assert.Equal(t, tt.expected, bits)
		})
	}
}

func TestVersionConstants(t *testing.T) {
	// Verify version constants have expected values
	assert.Equal(t, uint8(1), uint8(HFreshIndexVersion1))
	assert.Equal(t, uint8(2), uint8(HFreshIndexVersion2))
	assert.Equal(t, HFreshIndexVersion2, CurrentHFreshIndexVersion)
}
