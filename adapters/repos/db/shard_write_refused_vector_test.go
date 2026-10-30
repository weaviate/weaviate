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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestRefusedVectorLeavesNoObjectBehind pins that a write reported as failed stores
// nothing. An empty index can't validate a length, so the length of the first vector
// has to count from the moment that vector is accepted, not from when it is indexed.
func TestRefusedVectorLeavesNoObjectBehind(t *testing.T) {
	writeBatch := func(t *testing.T, shard ShardLike, objs []*storobj.Object) []error {
		return shard.PutObjectBatch(t.Context(), objs)
	}
	writeOneByOne := func(t *testing.T, shard ShardLike, objs []*storobj.Object) []error {
		errs := make([]error, len(objs))
		for i, obj := range objs {
			errs[i] = shard.PutObject(t.Context(), obj)
		}
		return errs
	}

	tests := []struct {
		name   string
		async  bool
		seeded bool
		write  func(t *testing.T, shard ShardLike, objs []*storobj.Object) []error
	}{
		{name: "sync, index holds a vector, batch", seeded: true, write: writeBatch},
		{name: "sync, index holds a vector, single", seeded: true, write: writeOneByOne},
		{name: "sync, empty index, batch", write: writeBatch},
		{name: "sync, empty index, single", write: writeOneByOne},
		{name: "async, first vector accepted, batch", async: true, seeded: true, write: writeBatch},
		{name: "async, first vector accepted, single", async: true, seeded: true, write: writeOneByOne},
		{name: "async, empty index, batch", async: true, write: writeBatch},
		{name: "async, empty index, single", async: true, write: writeOneByOne},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			className := "RefusedVector"
			flavor := consistencyFlavors()[0]
			shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
				enthnsw.NewDefaultUserConfig(), false, test.async)

			objs := consistencyObjects(flavor, className, 2)
			objs[1].Vector = []float32{1, 2, 3}

			if test.seeded {
				require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs[:1])))
				objs = objs[1:]
			}

			errs := test.write(t, shard, objs)

			refused := 0
			for i, obj := range objs {
				stored, err := shard.Exists(t.Context(), obj.ID())
				require.NoError(t, err)
				if errs[i] != nil {
					refused++
					assert.Falsef(t, stored, "object %d: a write reported as failed must leave nothing behind", i)
				} else {
					assert.Truef(t, stored, "object %d", i)
				}
			}
			assert.Equal(t, 1, refused, "exactly one of the two lengths has to lose")
		})
	}
}

// A skipped index holds no vectors, so nothing ties them to one length.
func TestSkippedIndexTakesVectorsOfAnyLength(t *testing.T) {
	className := "SkippedIndexLengths"
	flavor := consistencyFlavors()[0]
	shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
		enthnsw.UserConfig{Skip: true}, false, false)

	objs := consistencyObjects(flavor, className, 2)
	objs[1].Vector = []float32{1, 2, 3}

	require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))
}
