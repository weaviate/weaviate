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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestQuantizationDataSurvivesRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// capture quantization data before shutdown
	require.NotNil(t, index.quantizer)
	dataBefore := index.quantizer.Data()

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// reopen with the same store and config
	index2 := makeHFreshWithConfig(t, store, cfg, uc)

	require.NotNil(t, index2.quantizer)
	dataAfter := index2.quantizer.Data()

	require.Equal(t, dataBefore.InputDim, dataAfter.InputDim)
	require.Equal(t, dataBefore.Bits, dataAfter.Bits)
	require.Equal(t, dataBefore.Rounding, dataAfter.Rounding)

	require.Equal(t, dataBefore.Rotation.OutputDim, dataAfter.Rotation.OutputDim)
	require.Equal(t, dataBefore.Rotation.Rounds, dataAfter.Rotation.Rounds)
	require.Equal(t, dataBefore.Rotation.Swaps, dataAfter.Rotation.Swaps)
	require.Equal(t, dataBefore.Rotation.Signs, dataAfter.Rotation.Signs)
}

func TestRestoreMetadataBackfillsPartialPostingSizes(t *testing.T) {
	ctx := t.Context()
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	require.NoError(t, err)

	err = NewIndexMetadataStore(bucket).SetDimensions(64)
	require.NoError(t, err)

	postingMap := NewPostingMap(bucket)
	err = postingMap.SetVectorIDs(ctx, 10, Posting{
		NewVector(1, 1, nil),
		NewVector(2, 1, nil),
	})
	require.NoError(t, err)
	err = postingMap.SetVectorIDs(ctx, 11, Posting{
		NewVector(3, 1, nil),
		NewVector(4, 1, nil),
		NewVector(5, 1, nil),
	})
	require.NoError(t, err)

	partialSizes := NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a"))
	err = partialSizes.store.Set(ctx, 10, 2)
	require.NoError(t, err)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	size, err := index.PostingSizes.Get(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 2, size)

	size, err = index.PostingSizes.Get(ctx, 11)
	require.NoError(t, err)
	require.EqualValues(t, 3, size)

	persisted, err := index.PostingSizes.store.Get(ctx, 11)
	require.NoError(t, err)
	require.EqualValues(t, 3, persisted)
	require.EqualValues(t, 2, index.PostingSizes.Count())
	require.EqualValues(t, 5, index.PostingSizes.totalSize.Load())
}
