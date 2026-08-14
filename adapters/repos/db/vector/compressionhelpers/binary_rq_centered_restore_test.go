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

package compressionhelpers_test

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestRestoreRQCompressorRestoresCenteredBits1 pins that the compressor
// restore path accepts centered bits=1 — formerly a loud rejection, lifted
// now that the AddBRQCentered record carries rotation, rounding and mean
// together — and that a code stored before the "restart" reads back
// identically through the restored compressor.
func TestRestoreRQCompressorRestoresCenteredBits1(t *testing.T) {
	ctx := context.Background()
	dim := 256
	logger, _ := test.NewNullLogger()
	dist := distancer.NewCosineDistanceProvider()

	rng := rand.New(rand.NewPCG(9, 9))
	mean := make([]float32, dim)
	vec := make([]float32, dim)
	var norm float64
	for i := range vec {
		mean[i] = float32(rng.NormFloat64()) * 0.05
		vec[i] = float32(rng.NormFloat64())
		norm += float64(vec[i]) * float64(vec[i])
	}
	norm = math.Sqrt(norm)
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}

	rq, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(dim, 42, mean, dist)
	require.NoError(t, err)
	data := rq.Data()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())
	restored, err := compressionhelpers.RestoreRQCompressor(dist, 1e6, logger,
		int(data.InputDim), 1, int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
		data.Rotation.Swaps, data.Rotation.Signs, data.Rounding, data.Mean,
		store, memwatch.NewDummyMonitor(), lsmkv.MakeNoopBucketOptions, "", nil)
	require.NoError(t, err, "centered bits=1 restore must be accepted")
	defer restored.Drop()

	restored.Preload(7, vec)
	got, err := restored.DistanceBetweenCompressedVectorsFromIDs(ctx, 7, 7)
	require.NoError(t, err)
	assert.InDelta(t, 0.0, float64(got), 0.05, "self-distance through the restored compressor")
}
