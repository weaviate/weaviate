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

//go:build integrationTest

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestObjectVectorSearch_MissingObjectKeepsDistancesAligned pins the case where
// the vector index returns a doc ID whose object is no longer in the objects
// bucket (deleted behind the index's back, e.g. by an external delete or a
// torn delete before the tombstone landed). The missing object must be dropped
// together with its distance, so every returned object keeps its own distance.
func TestObjectVectorSearch_MissingObjectKeepsDistancesAligned(t *testing.T) {
	const numObjects = 5

	// query along the first axis; object i sits at an increasing angle, so the
	// true ranking is 0,1,2,3,4
	query := []float32{1, 0, 0}
	vectors := make([][]float32, numObjects)
	for i := range vectors {
		vectors[i] = normalized([]float32{1, float32(i) * 0.3, 0})
	}

	cases := []struct {
		name        string
		missingRank int
	}{
		{name: "missing object is the best hit", missingRank: 0},
		{name: "missing object is in the middle", missingRank: 2},
		{name: "missing object is the worst hit", missingRank: numObjects - 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			className := fmt.Sprintf("MissingObjRank%d", tc.missingRank)
			shard, index := testShardWithSettings(t, ctx, &models.Class{Class: className},
				enthnsw.NewDefaultUserConfig(), false, false, false)
			defer func() {
				require.NoError(t, index.drop())
			}()

			ids := make([]strfmt.UUID, numObjects)
			for i := range vectors {
				obj := testObject(className)
				obj.Vector = vectors[i]
				require.NoError(t, shard.PutObject(ctx, obj))
				ids[i] = obj.ID()
			}

			deleteFromObjectsBucketOnly(t, shard, ids[tc.missingRank])

			objs, dists, err := shard.ObjectVectorSearch(ctx, []models.Vector{query}, []string{""},
				0, numObjects*2, nil, nil, nil, additional.Properties{}, nil, nil, nil)
			require.NoError(t, err)

			require.Len(t, objs, numObjects-1, "the missing object must be dropped")
			require.Len(t, dists, len(objs), "one distance per returned object")

			vectorByID := map[strfmt.UUID][]float32{}
			for i, id := range ids {
				vectorByID[id] = vectors[i]
			}
			for i, obj := range objs {
				require.NotEqual(t, ids[tc.missingRank], obj.ID(), "missing object must not be returned")
				want := 1 - dot(query, vectorByID[obj.ID()])
				require.InDelta(t, want, dists[i], 1e-5,
					"object at position %d (%s) carries another object's distance", i, obj.ID())
				if i > 0 {
					require.LessOrEqual(t, dists[i-1], dists[i], "distances must stay sorted")
				}
			}
		})
	}
}

// deleteFromObjectsBucketOnly removes the object's row (and its doc ID
// secondary key) from the objects bucket without touching the vector index,
// so the HNSW node stays alive and untombstoned.
func deleteFromObjectsBucketOnly(t *testing.T, shard ShardLike, id strfmt.UUID) {
	t.Helper()
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	require.NoError(t, err)

	bucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	existing, err := bucket.Get(idBytes)
	require.NoError(t, err)
	require.NotNil(t, existing)

	docID, _, err := storobj.DocIDAndTimeFromBinary(existing)
	require.NoError(t, err)
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	require.NoError(t, bucket.Delete(idBytes,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
}

func normalized(v []float32) []float32 {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	norm := float32(math.Sqrt(float64(sum)))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x / norm
	}
	return out
}

func dot(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}
