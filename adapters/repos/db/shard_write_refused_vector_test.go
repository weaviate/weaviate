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
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

// mergeRoutes are the two ways a merge reaches a shard. A replicated class skips
// MergeObject, which is why what they share has to be tested through both.
var mergeRoutes = []struct {
	name  string
	merge func(ctx context.Context, shard ShardLike, doc objects.MergeDocument) error
}{
	{"merge object", func(ctx context.Context, shard ShardLike, doc objects.MergeDocument) error {
		return shard.MergeObject(ctx, doc)
	}},
	{"replication", func(ctx context.Context, shard ShardLike, doc objects.MergeDocument) error {
		idBytes, err := uuid.MustParse(doc.ID.String()).MarshalBinary()
		if err != nil {
			return err
		}
		return shard.(*Shard).merge(ctx, idBytes, doc)
	}},
}

func mergeOfVector(flavor consistencyFlavor, obj *storobj.Object, vector []float32) objects.MergeDocument {
	doc := objects.MergeDocument{Class: obj.Class().String(), ID: obj.ID(), UpdateTime: 2}
	switch {
	case flavor.target == "":
		doc.Vector = vector
	case flavor.name == "multi vector":
		doc.Vectors = models.Vectors{flavor.target: [][]float32{vector, vector}}
	default:
		doc.Vectors = models.Vectors{flavor.target: vector}
	}
	return doc
}

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

// TestRefusedMergeLeavesObjectUnchanged pins the same for a merge, which doesn't pass
// through the validation of a put: refused means the stored object is still the old one.
func TestRefusedMergeLeavesObjectUnchanged(t *testing.T) {
	for _, flavor := range consistencyFlavors() {
		for _, route := range mergeRoutes {
			for _, async := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s, %s, async %v", flavor.name, route.name, async), func(t *testing.T) {
					className := "RefusedMerge"
					shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
						enthnsw.NewDefaultUserConfig(), false, async, flavor.opts...)

					objs := consistencyObjects(flavor, className, 2)
					require.NoError(t, shard.PutObject(t.Context(), objs[0]))

					objs[1].Vector, objs[1].Vectors, objs[1].MultiVectors = nil, nil, nil
					require.NoError(t, shard.PutObject(t.Context(), objs[1]))

					doc := mergeOfVector(flavor, objs[1], []float32{1, 2, 3})
					require.Error(t, route.merge(t.Context(), shard, doc))

					stored, err := shard.ObjectByID(t.Context(), objs[1].ID(), nil, additional.Properties{Vector: true})
					require.NoError(t, err)
					require.NotNil(t, stored)
					assert.Empty(t, flavor.get(stored), "a refused merge must not store its vector")
					assert.EqualValues(t, 1, stored.LastUpdateTimeUnix(), "a refused merge must not touch the object")

					// the right length is still welcome
					doc = mergeOfVector(flavor, objs[1], consistencyVector(1))
					require.NoError(t, route.merge(t.Context(), shard, doc))
				})
			}
		}
	}
}

// TestRefusedWriteClaimsNoLength pins that a refused write decides nothing for an index
// that is still empty, where the first length accepted stands in for its dimensions.
func TestRefusedWriteClaimsNoLength(t *testing.T) {
	const className = "RefusedWriteLengths"
	wrong := []float32{1, 2, 3}

	withVectors := func(obj *storobj.Object, vectors map[string][]float32) *storobj.Object {
		obj.Object.Properties = map[string]interface{}{"title": "object"}
		obj.Object.CreationTimeUnix, obj.Object.LastUpdateTimeUnix = 1, 1
		obj.Vector, obj.Vectors = nil, vectors
		return obj
	}

	journeys := []struct {
		name  string
		write func(t *testing.T, shard ShardLike, obj *storobj.Object) error
	}{
		{"put", func(t *testing.T, shard ShardLike, obj *storobj.Object) error {
			return shard.PutObject(t.Context(), withVectors(obj, map[string][]float32{"a": wrong, "b": wrong}))
		}},
		{"batch", func(t *testing.T, shard ShardLike, obj *storobj.Object) error {
			errs := shard.PutObjectBatch(t.Context(), []*storobj.Object{
				withVectors(obj, map[string][]float32{"a": wrong, "b": wrong}),
			})
			return errs[0]
		}},
	}
	for _, route := range mergeRoutes {
		journeys = append(journeys, struct {
			name  string
			write func(t *testing.T, shard ShardLike, obj *storobj.Object) error
		}{route.name, func(t *testing.T, shard ShardLike, obj *storobj.Object) error {
			require.NoError(t, shard.PutObject(t.Context(), withVectors(obj, nil)))
			return route.merge(t.Context(), shard, objects.MergeDocument{
				Class: className, ID: obj.ID(), UpdateTime: 2,
				Vectors: models.Vectors{"a": wrong, "b": wrong},
			})
		}})
	}

	for _, journey := range journeys {
		for _, async := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, async %v", journey.name, async), func(t *testing.T) {
				shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
					enthnsw.NewDefaultUserConfig(), false, async, func(i *Index) {
						i.vectorIndexUserConfig = nil
						i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
							"a": enthnsw.NewDefaultUserConfig(),
							"b": enthnsw.NewDefaultUserConfig(),
						}
					})

				objs := batchOfObjects(className, 42)
				// b is in use, a is still empty
				first := withVectors(objs[0], map[string][]float32{"b": consistencyVector(0)})
				require.NoError(t, shard.PutObject(t.Context(), first))

				// maps have no order, so one attempt reaches a before b only some of the time
				for _, obj := range objs[1:41] {
					require.Error(t, journey.write(t, shard, obj), "the length of b has to be refused")
				}

				last := withVectors(objs[41], map[string][]float32{"a": consistencyVector(1), "b": consistencyVector(1)})
				assert.NoError(t, shard.PutObject(t.Context(), last))
			})
		}
	}
}

// A merge of an object that isn't there writes nothing, so it can't decide a length.
func TestMergeOfMissingObjectClaimsNoLength(t *testing.T) {
	for _, route := range mergeRoutes {
		for _, async := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, async %v", route.name, async), func(t *testing.T) {
				className := "MissingObjectLengths"
				flavor := consistencyFlavors()[0]
				shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
					enthnsw.NewDefaultUserConfig(), false, async)

				objs := consistencyObjects(flavor, className, 2)
				doc := mergeOfVector(flavor, objs[1], []float32{1, 2, 3})
				require.ErrorIs(t, route.merge(t.Context(), shard, doc), errObjectNotFound)

				assert.NoError(t, shard.PutObject(t.Context(), objs[0]))
			})
		}
	}
}

// A merge of properties carries the stored vector over. That vector was accepted long
// ago and isn't being written, so a length claimed since must not get in its way.
func TestMergeOfPropertiesClaimsNoLength(t *testing.T) {
	className := "MergedPropertiesLengths"
	flavor := consistencyFlavors()[0]
	shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
		enthnsw.NewDefaultUserConfig(), false, true)

	objs := consistencyObjects(flavor, className, 1)
	require.NoError(t, shard.PutObject(t.Context(), objs[0]))

	_, err := shard.(*Shard).WithVectorIndexQueue("", func(queue *VectorIndexQueue) error {
		queue.dims.Store(3)
		return nil
	})
	require.NoError(t, err)

	assert.NoError(t, shard.MergeObject(t.Context(), objects.MergeDocument{
		Class: className, ID: objs[0].ID(), UpdateTime: 2,
		PrimitiveSchema: map[string]interface{}{"title": "renamed"},
	}))
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
