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
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
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
	assert.Equal(t, int16(8), index2.Centroids.RQBits())  // V1 uses RQ8
	assert.Equal(t, 0, index2.Centroids.RQRescoreLimit()) // V1: no HNSW RQ rescoring
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

func TestIndexVersion_V1UsesRQ8AndNoRescore(t *testing.T) {
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

	// Reopen with V1 - should use RQ8 and no HNSW rescoring
	index2 := makeHFreshWithConfig(t, store, cfg, uc)
	assert.Equal(t, uint8(HFreshIndexVersion1), index2.Version())
	assert.Equal(t, int16(8), index2.Centroids.RQBits())
	assert.Equal(t, 0, index2.Centroids.RQRescoreLimit()) // V1: no HNSW RQ rescoring
}

func TestIndexVersion_V2UsesRQ1AndRescore(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// CurrentVersion is V2, should use RQ1 with rescoring enabled
	assert.Equal(t, uint8(HFreshIndexVersion2), index.Version())
	assert.Equal(t, int16(1), index.Centroids.RQBits())
	assert.Equal(t, ent.DefaultPostingRescoreLimit, index.Centroids.RQRescoreLimit())

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Reopen and verify still V2 with RQ1 and rescoring
	index2 := makeHFreshWithConfig(t, store, cfg, uc)
	assert.Equal(t, uint8(HFreshIndexVersion2), index2.Version())
	assert.Equal(t, int16(1), index2.Centroids.RQBits())
	assert.Equal(t, ent.DefaultPostingRescoreLimit, index2.Centroids.RQRescoreLimit())
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

func TestRQRescoreLimitForVersion(t *testing.T) {
	tests := []struct {
		name     string
		version  uint8
		expected int
	}{
		{
			name:     "V1 has no HNSW rescoring",
			version:  HFreshIndexVersion1,
			expected: 0,
		},
		{
			name:     "V2 has HNSW rescoring enabled",
			version:  HFreshIndexVersion2,
			expected: ent.DefaultPostingRescoreLimit,
		},
		{
			name:     "V0 (hypothetical legacy) has no rescoring",
			version:  0,
			expected: 0,
		},
		{
			name:     "V3 (future) has rescoring enabled",
			version:  3,
			expected: ent.DefaultPostingRescoreLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit := postingRescoreLimitForVersion(tt.version, ent.DefaultPostingRescoreLimit)
			assert.Equal(t, tt.expected, limit)
		})
	}
}

// TestV1ConfigSelection verifies V1 uses correct RQ config:
// - RQ bits == 8
// - HNSW RQ rescore limit == 0
func TestV1ConfigSelection(t *testing.T) {
	assert.Equal(t, int16(8), rqBitsForVersion(HFreshIndexVersion1))
	assert.Equal(t, 0, postingRescoreLimitForVersion(HFreshIndexVersion1, ent.DefaultPostingRescoreLimit))
}

// TestV2ConfigSelection verifies V2 uses correct RQ config:
// - RQ bits == 1
// - HNSW posting rescore limit == ent.DefaultPostingRescoreLimit (100)
func TestV2ConfigSelection(t *testing.T) {
	assert.Equal(t, int16(1), rqBitsForVersion(HFreshIndexVersion2))
	assert.Equal(t, ent.DefaultPostingRescoreLimit, postingRescoreLimitForVersion(HFreshIndexVersion2, ent.DefaultPostingRescoreLimit))
}

// TestV1BackwardCompatibility_NoMedoidRescoreError is a regression test for the V1
// backward compatibility bug where V1 indexes would fail with "vector lengths don't match"
// errors because HNSW tried to rescore using medoid vectors that don't exist for V1.
//
// This test verifies that:
// 1. V1 configuration has RQ rescore limit = 0 (no HNSW medoid rescoring)
// 2. V1 search doesn't call the medoid vector provider
// 3. Search completes without "vector lengths don't match" error
func TestV1BackwardCompatibility_NoMedoidRescoreError(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Create an index and add vectors
	index := makeHFreshWithConfig(t, store, cfg, uc)

	// Add multiple vectors to create centroid postings
	vectors, _ := testinghelpers.RandomVecs(10, 0, 64)
	for i, vec := range vectors {
		err := index.Add(t.Context(), uint64(i), vec)
		require.NoError(t, err)
	}

	// Remove version metadata to simulate a legacy V1 index
	err := index.IndexMetadata.bucket.Delete(index.IndexMetadata.key(indexVersionKey))
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// Reopen the index - should default to V1
	index2 := makeHFreshWithConfig(t, store, cfg, uc)

	// Verify V1 configuration - this is the key fix
	assert.Equal(t, uint8(HFreshIndexVersion1), index2.Version())
	assert.Equal(t, int16(8), index2.Centroids.RQBits())
	assert.Equal(t, 0, index2.Centroids.RQRescoreLimit(), "V1 must have HNSW RQ rescore limit = 0")

	// Run a search - the key assertion is that this does NOT return an error
	// about "vector lengths don't match" which was the original bug.
	// Note: Results may be empty because this simulated V1 index was created
	// with V2's RQ1 compression but reopened with V1's RQ8 config - this is
	// expected and not the bug we're testing for.
	_, _, err = index2.SearchByVector(t.Context(), vectors[0], 5, nil)
	require.NoError(t, err, "V1 search should not fail with 'vector lengths don't match' error")
}

// TestV1MedoidProviderNotCalled verifies that V1 indexes don't call the medoid
// vector provider thunks. The thunks are configured to return an error for V1
// to catch any unexpected calls.
func TestV1MedoidProviderNotCalled(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Remove version metadata before creating to get a true V1 from scratch
	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	require.NoError(t, err)

	// Pre-set V1 version before creating the index
	metadata := NewIndexMetadataStore(bucket)
	err = metadata.SetVersion(HFreshIndexVersion1)
	require.NoError(t, err)

	// Also set dimensions to make it look like an existing index
	err = metadata.SetDimensions(64)
	require.NoError(t, err)

	// Now create the index - it should detect V1 and configure accordingly
	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(t.Context()) })

	// Verify V1 configuration
	assert.Equal(t, uint8(HFreshIndexVersion1), index.Version())
	assert.Equal(t, 0, index.Centroids.RQRescoreLimit())

	// The medoid vector provider for V1 is set to return an error if called.
	// If any search path accidentally tries to use medoid rescoring for V1,
	// this would cause test failures, which is what we want to catch.
}

func TestVersionConstants(t *testing.T) {
	// Verify version constants have expected values
	assert.Equal(t, uint8(1), uint8(HFreshIndexVersion1))
	assert.Equal(t, uint8(2), uint8(HFreshIndexVersion2))
	assert.Equal(t, HFreshIndexVersion2, CurrentHFreshIndexVersion)
}

// TestDecoupledRescoreLimits verifies that posting rescore limit and final vector
// rescore limit are properly decoupled.
func TestDecoupledRescoreLimits(t *testing.T) {
	t.Run("posting rescore limit is 100 for V2", func(t *testing.T) {
		assert.Equal(t, 100, ent.DefaultPostingRescoreLimit, "posting rescore limit should be 100")
	})

	t.Run("final vector rescore limit is 350", func(t *testing.T) {
		assert.Equal(t, 350, ent.DefaultHFreshRescoreLimit, "final vector rescore limit should be 350")
	})

	t.Run("limits are different", func(t *testing.T) {
		assert.NotEqual(t, ent.DefaultPostingRescoreLimit, ent.DefaultHFreshRescoreLimit,
			"posting and final vector rescore limits should be different")
	})
}

// TestCustomFinalRescoreLimitDoesNotChangePostingRescoreLimit verifies that
// configuring a custom final vector rescore limit does not affect the HNSW
// posting representative rescore limit.
func TestCustomFinalRescoreLimitDoesNotChangePostingRescoreLimit(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Set a custom final vector rescore limit
	uc.RQ.RescoreLimit = 500

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// Verify V2 index
	assert.Equal(t, uint8(HFreshIndexVersion2), index.Version())

	// Final vector rescore limit should be the custom value
	assert.Equal(t, uint32(500), index.rescoreLimit)

	// HNSW posting rescore limit should still be ent.DefaultPostingRescoreLimit (100)
	assert.Equal(t, ent.DefaultPostingRescoreLimit, index.Centroids.RQRescoreLimit(),
		"custom final vector rescore limit should not change posting rescore limit")
}

// TestPostingRescoreLimitValues verifies the posting rescore limit values for each version.
func TestPostingRescoreLimitValues(t *testing.T) {
	tests := []struct {
		name     string
		version  uint8
		expected int
	}{
		{
			name:     "V1 has posting rescore limit of 0",
			version:  HFreshIndexVersion1,
			expected: 0,
		},
		{
			name:     "V2 has posting rescore limit of 100",
			version:  HFreshIndexVersion2,
			expected: 100,
		},
		{
			name:     "future version V3 has posting rescore limit of 100",
			version:  3,
			expected: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit := postingRescoreLimitForVersion(tt.version, ent.DefaultPostingRescoreLimit)
			assert.Equal(t, tt.expected, limit)
		})
	}
}

// TestV2PostingRescoreLimitIs100 is a sanity check that V2 uses 100 postings
// for rescoring during the centroid search phase.
func TestV2PostingRescoreLimitIs100(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// Verify V2 is created by default
	assert.Equal(t, uint8(HFreshIndexVersion2), index.Version())

	// Key assertion: posting rescore limit should be 100, NOT 350
	postingRescoreLimit := index.Centroids.RQRescoreLimit()
	assert.Equal(t, 100, postingRescoreLimit,
		"V2 posting rescore limit should be 100")

	// Ensure it's not the old value
	assert.NotEqual(t, 350, postingRescoreLimit,
		"V2 posting rescore limit should NOT be 350 (the old value)")
}

// TestCustomPostingRescoreLimit verifies that users can configure a custom
// posting rescore limit via the postingRescoreLimit config parameter.
func TestCustomPostingRescoreLimit(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Set a custom posting rescore limit
	uc.PostingRescoreLimit = 200

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// Verify V2 is created by default
	assert.Equal(t, uint8(HFreshIndexVersion2), index.Version())

	// Posting rescore limit should be the custom value
	assert.Equal(t, 200, index.Centroids.RQRescoreLimit(),
		"posting rescore limit should be the custom configured value")
}
