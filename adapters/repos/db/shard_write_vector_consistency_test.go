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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

const consistencyVectorDims = 16

// consistencyFlavor is one way of attaching a vector to an object.
type consistencyFlavor struct {
	name   string
	target string
	opts   []func(*Index)
	set    func(obj *storobj.Object, vector []float32)
	get    func(obj *storobj.Object) models.Vector
}

func namedVectorOpts(target string, config schemaConfig.VectorIndexConfig) []func(*Index) {
	return []func(*Index){func(i *Index) {
		i.vectorIndexUserConfig = nil
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{target: config}
	}}
}

func namedVectorFlavor(name string, config schemaConfig.VectorIndexConfig) consistencyFlavor {
	return consistencyFlavor{
		name:   name,
		target: "default",
		opts:   namedVectorOpts("default", config),
		set: func(obj *storobj.Object, vector []float32) {
			obj.Vector = nil
			obj.Vectors = map[string][]float32{"default": vector}
		},
		get: func(obj *storobj.Object) models.Vector { return obj.Vectors["default"] },
	}
}

func consistencyFlavors() []consistencyFlavor {
	multi := enthnsw.NewDefaultUserConfig()
	multi.Multivector = enthnsw.MultivectorConfig{Enabled: true}

	return []consistencyFlavor{
		{
			name: "legacy vector",
			set:  func(obj *storobj.Object, vector []float32) { obj.Vector = vector },
			get:  func(obj *storobj.Object) models.Vector { return obj.Vector },
		},
		namedVectorFlavor("named vector", enthnsw.NewDefaultUserConfig()),
		namedVectorFlavor("flat named vector", flat.NewDefaultUserConfig()),
		{
			name:   "multi vector",
			target: "colbert",
			opts:   namedVectorOpts("colbert", multi),
			set: func(obj *storobj.Object, vector []float32) {
				obj.Vector = nil
				obj.MultiVectors = map[string][][]float32{"colbert": {vector, vector}}
			},
			get: func(obj *storobj.Object) models.Vector { return obj.MultiVectors["colbert"] },
		},
	}
}

func consistencyClass(name string) *models.Class {
	return &models.Class{
		Class:               name,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:         "title",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
		}},
	}
}

func consistencyVector(seed int) []float32 {
	vector := make([]float32, consistencyVectorDims)
	for i := range vector {
		vector[i] = float32(((seed+1)*(i+3))%97) + 1
	}
	return vector
}

func consistencyObjects(flavor consistencyFlavor, className string, count int) []*storobj.Object {
	objs := batchOfObjects(className, count)
	for i, obj := range objs {
		obj.Object.Properties = map[string]interface{}{"title": fmt.Sprintf("object %d", i)}
		obj.Object.CreationTimeUnix = 1
		obj.Object.LastUpdateTimeUnix = 1
		flavor.set(obj, consistencyVector(i))
	}
	return objs
}

// newerVersions returns the objects the way a client resends them: same content
// and, when the server stamps the write, a later update time.
func newerVersions(objs []*storobj.Object, bump int64) []*storobj.Object {
	out := make([]*storobj.Object, len(objs))
	for i, obj := range objs {
		cp := *obj
		cp.Object.LastUpdateTimeUnix += bump
		out[i] = &cp
	}
	return out
}

func withNewVectors(flavor consistencyFlavor, objs []*storobj.Object, offset int) []*storobj.Object {
	out := newerVersions(objs, 1)
	for i, obj := range out {
		flavor.set(obj, consistencyVector(offset+i))
	}
	return out
}

// requireSelfHits runs the probe of issue #13609: every stored object has to be
// the nearest neighbor of its own stored vector.
func requireSelfHits(t *testing.T, shard ShardLike, flavor consistencyFlavor, objs []*storobj.Object) {
	t.Helper()

	var targets []string
	var addl additional.Properties
	if flavor.target == "" {
		targets = []string{""}
		addl.Vector = true
	} else {
		targets = []string{flavor.target}
		addl.Vectors = targets
	}

	for i, obj := range objs {
		stored, err := shard.ObjectByID(t.Context(), obj.ID(), nil, addl)
		require.NoError(t, err)
		if stored == nil {
			continue
		}

		vector := flavor.get(stored)
		require.NotEmptyf(t, vector, "object %d is stored without its vector", i)
		res, _, err := shard.ObjectVectorSearch(t.Context(), []models.Vector{vector}, targets, 0,
			len(objs), nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)

		ids := make([]string, len(res))
		for j, r := range res {
			ids[j] = r.ID().String()
		}
		assert.Containsf(t, ids, obj.ID().String(),
			"object %d is readable but its own vector does not find it", i)
	}
}

// cancelOnObjectWrite cancels once an object is in the LSM store: this is what a
// replica sees when the coordinator goes away during a commit.
type cancelOnObjectWrite struct {
	ShardLike
	cancel context.CancelFunc
}

func (s *cancelOnObjectWrite) putObjectLSM(ctx context.Context, object *storobj.Object,
	idBytes []byte,
) (objectInsertStatus, error) {
	status, err := s.ShardLike.putObjectLSM(ctx, object, idBytes)
	s.cancel()
	return status, err
}

// cancelBetweenStages cancels after the LSM stage, while the batch tombstones the
// documents it replaced.
type cancelBetweenStages struct {
	ShardLike
	cancel context.CancelFunc
}

func (s *cancelBetweenStages) ForEachVectorQueue(f func(string, *VectorIndexQueue) error) error {
	err := s.ShardLike.ForEachVectorQueue(f)
	s.cancel()
	return err
}

// cancelAfterVectors cancels once the given number of vectors was handed to the
// vector index.
type cancelAfterVectors struct {
	ShardLike
	cancel    context.CancelFunc
	remaining atomic.Int32
}

func (s *cancelAfterVectors) spend() {
	if s.remaining.Add(-1) < 0 {
		s.cancel()
	}
}

func (s *cancelAfterVectors) updateVectorIndexIgnoreDelete(ctx context.Context, vector []float32,
	status objectInsertStatus,
) error {
	s.spend()
	return s.ShardLike.updateVectorIndexIgnoreDelete(ctx, vector, status)
}

func (s *cancelAfterVectors) updateVectorIndexesIgnoreDelete(ctx context.Context,
	vectors map[string][]float32, status objectInsertStatus,
) error {
	s.spend()
	return s.ShardLike.updateVectorIndexesIgnoreDelete(ctx, vectors, status)
}

func (s *cancelAfterVectors) updateMultiVectorIndexesIgnoreDelete(ctx context.Context,
	vectors map[string][][]float32, status objectInsertStatus,
) error {
	s.spend()
	return s.ShardLike.updateMultiVectorIndexesIgnoreDelete(ctx, vectors, status)
}

func runBatch(ctx context.Context, shard ShardLike, objs []*storobj.Object) []error {
	batcher := newObjectsBatcher(shard, shard.Index().logger)
	errs := batcher.Objects(ctx, objs)
	batcher.wg.Wait()
	return errs
}

func putEach(ctx context.Context, shard ShardLike, objs []*storobj.Object) []error {
	errs := make([]error, len(objs))
	for i, obj := range objs {
		errs[i] = shard.PutObject(ctx, obj)
	}
	return errs
}

func mergeVectors(ctx context.Context, shard ShardLike, flavor consistencyFlavor, objs []*storobj.Object) {
	for _, obj := range objs {
		doc := objects.MergeDocument{
			Class: obj.Class().String(), ID: obj.ID(),
			UpdateTime: obj.Object.LastUpdateTimeUnix,
		}
		if flavor.target == "" {
			doc.Vector = obj.Vector
		} else {
			doc.Vectors = models.Vectors{flavor.target: flavor.get(obj)}
		}
		_ = shard.MergeObject(ctx, doc)
	}
}

func mergeTitles(t *testing.T, shard ShardLike, objs []*storobj.Object) {
	t.Helper()

	for i, obj := range objs {
		require.NoError(t, shard.MergeObject(t.Context(), objects.MergeDocument{
			Class: obj.Class().String(), ID: obj.ID(), UpdateTime: 100,
			PrimitiveSchema: map[string]interface{}{"title": fmt.Sprintf("merged %d", i)},
		}))
	}
}

func prepareAndCommitEach(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard *Shard,
	objs []*storobj.Object,
) {
	t.Helper()

	for i, obj := range objs {
		requestID := fmt.Sprintf("put-%d", i)
		require.Empty(t, shard.preparePutObject(t.Context(), requestID, obj).Errors)
	}
	cancel()
	for i := range objs {
		shard.commitReplication(ctx, fmt.Sprintf("put-%d", i))
	}
}

// TestCancelledWriteLeavesNoObjectOutsideTheVectorIndex pins issue #13609 at its
// source. The object store and the vector index are written one after the other,
// so a write that stops in between leaves a readable object that search can't
// find. Nothing repairs it afterwards: async replication compares the object
// store only.
func TestCancelledWriteLeavesNoObjectOutsideTheVectorIndex(t *testing.T) {
	const count = 9

	tests := []struct {
		name string
		// seeded stores the objects first, so the write is an update
		seeded bool
		write  func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
			flavor consistencyFlavor, objs []*storobj.Object) []*storobj.Object
	}{
		{
			name: "new batch, cancelled while the objects are stored",
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				_ consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				runBatch(ctx, &cancelOnObjectWrite{ShardLike: shard, cancel: cancel}, objs)
				return objs
			},
		},
		{
			name: "new batch, cancelled after one vector",
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				_ consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				wrapped := &cancelAfterVectors{ShardLike: shard, cancel: cancel}
				wrapped.remaining.Store(1)
				runBatch(ctx, wrapped, objs)
				return objs
			},
		},
		{
			name:   "batch update, cancelled between the stages",
			seeded: true,
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				flavor consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				updated := withNewVectors(flavor, objs, count)
				errs := runBatch(ctx, &cancelBetweenStages{ShardLike: shard, cancel: cancel}, updated)
				require.Empty(t, nonNilErrs(errs), "stored objects have to reach the vector index")
				return updated
			},
		},
		{
			name: "single new object, cancelled",
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				_ consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				cancel()
				putEach(ctx, shard, objs)
				return objs
			},
		},
		{
			name:   "single update, cancelled",
			seeded: true,
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				flavor consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				updated := withNewVectors(flavor, objs, count)
				cancel()
				putEach(ctx, shard, updated)
				return updated
			},
		},
		{
			name:   "merge changing the vector, cancelled",
			seeded: true,
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				flavor consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				updated := withNewVectors(flavor, objs, count)
				cancel()
				mergeVectors(ctx, shard, flavor, updated)
				return updated
			},
		},
		{
			name: "replica commit of a single object, cancelled",
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				_ consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				prepareAndCommitEach(t, ctx, cancel, underlyingShard(t, shard), objs)
				return objs
			},
		},
		{
			name: "replica commit of a batch, cancelled",
			write: func(t *testing.T, ctx context.Context, cancel context.CancelFunc, shard ShardLike,
				_ consistencyFlavor, objs []*storobj.Object,
			) []*storobj.Object {
				local := underlyingShard(t, shard)
				require.Empty(t, local.preparePutObjects(t.Context(), "batch", objs).Errors)
				cancel()
				local.commitReplication(ctx, "batch")
				return objs
			},
		},
	}

	for _, flavor := range consistencyFlavors() {
		for _, test := range tests {
			t.Run(flavor.name+"/"+test.name, func(t *testing.T) {
				className := "CancelledWrite"
				shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
					enthnsw.NewDefaultUserConfig(), false, false, flavor.opts...)

				objs := consistencyObjects(flavor, className, count)
				if test.seeded {
					require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))
					requireSelfHits(t, shard, flavor, objs)
				}

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				written := test.write(t, ctx, cancel, shard, flavor, objs)

				requireSelfHits(t, shard, flavor, written)
			})
		}
	}
}

// storeWithoutVector leaves the object in the state issue #13609 reports: in the
// object store, unknown to the vector index.
func storeWithoutVector(t *testing.T, shard ShardLike, obj *storobj.Object) {
	t.Helper()

	id, err := uuid.Parse(obj.ID().String())
	require.NoError(t, err)
	idBytes, err := id.MarshalBinary()
	require.NoError(t, err)

	_, err = shard.putObjectLSM(t.Context(), obj, idBytes)
	require.NoError(t, err)
}

func restartedShard(t *testing.T, idx *Index, shard ShardLike) ShardLike {
	t.Helper()

	require.NoError(t, shard.Shutdown(t.Context()))
	idx.shards.LoadAndDelete(shard.Name())
	restarted, release, err := idx.getOrInitShard(t.Context(), shard.Name())
	require.NoError(t, err)
	t.Cleanup(release)
	return restarted
}

func cleanUpTombstones(t *testing.T, shard ShardLike, flavor consistencyFlavor) {
	t.Helper()

	found, err := shard.WithVectorIndex(flavor.target, func(index VectorIndex) error {
		if !hnsw.IsHNSWIndex(index) {
			return nil
		}
		return hnsw.AsHNSWIndex(index).CleanUpTombstonedNodes(func() bool { return false })
	})
	require.True(t, found)
	require.NoError(t, err)
}

// TestRewriteRestoresObjectMissingFromVectorIndex pins the repair of shards that
// are already affected: resending an unchanged object has to index what is
// missing. Before, the write was skipped as a no-op and reported success.
func TestRewriteRestoresObjectMissingFromVectorIndex(t *testing.T) {
	const count = 9

	tests := []struct {
		name    string
		indexed int
		rewrite func(t *testing.T, shard ShardLike, flavor consistencyFlavor, objs []*storobj.Object)
	}{
		{
			name:    "batch with the same update time, nothing indexed",
			indexed: 0,
			rewrite: func(t *testing.T, shard ShardLike, _ consistencyFlavor, objs []*storobj.Object) {
				require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))
			},
		},
		{
			name:    "batch with the same update time, one object indexed",
			indexed: 1,
			rewrite: func(t *testing.T, shard ShardLike, _ consistencyFlavor, objs []*storobj.Object) {
				require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))
			},
		},
		{
			name:    "batch with a newer update time",
			indexed: 1,
			rewrite: func(t *testing.T, shard ShardLike, _ consistencyFlavor, objs []*storobj.Object) {
				require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), newerVersions(objs, 10))))
			},
		},
		{
			name:    "single put with the same update time",
			indexed: 1,
			rewrite: func(t *testing.T, shard ShardLike, _ consistencyFlavor, objs []*storobj.Object) {
				require.Empty(t, nonNilErrs(putEach(t.Context(), shard, objs)))
			},
		},
		{
			name:    "single put with a newer update time",
			indexed: 1,
			rewrite: func(t *testing.T, shard ShardLike, _ consistencyFlavor, objs []*storobj.Object) {
				require.Empty(t, nonNilErrs(putEach(t.Context(), shard, newerVersions(objs, 10))))
			},
		},
		{
			name:    "merge that leaves the vector alone",
			indexed: 1,
			rewrite: func(t *testing.T, shard ShardLike, _ consistencyFlavor, objs []*storobj.Object) {
				mergeTitles(t, shard, objs)
			},
		},
	}

	for _, flavor := range consistencyFlavors() {
		for _, test := range tests {
			t.Run(flavor.name+"/"+test.name, func(t *testing.T) {
				className := "RewriteRepair"
				shard, idx := testShardWithSettings(t, t.Context(), consistencyClass(className),
					enthnsw.NewDefaultUserConfig(), false, false, flavor.opts...)

				objs := consistencyObjects(flavor, className, count)
				if test.indexed > 0 {
					require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs[:test.indexed])))
				}
				for _, obj := range objs[test.indexed:] {
					storeWithoutVector(t, shard, obj)
				}

				test.rewrite(t, shard, flavor, objs)
				requireSelfHits(t, shard, flavor, objs)

				cleanUpTombstones(t, shard, flavor)
				requireSelfHits(t, shard, flavor, objs)

				requireSelfHits(t, restartedShard(t, idx, shard), flavor, objs)
			})
		}
	}
}

// ghostingIndex stands in for an hnsw index holding a ghost, which only a failed
// insert can create.
type ghostingIndex struct {
	VectorIndex
	ghost uint64
}

func (i *ghostingIndex) HasGhost(docID uint64) bool {
	return docID == i.ghost
}

// TestRewriteReplacesDocOfGhost pins that a rewrite repairs a document the index
// holds but can't reach, which ContainsDoc doesn't tell apart from a healthy one.
func TestRewriteReplacesDocOfGhost(t *testing.T) {
	const count = 3

	className := "RewriteGhost"
	flavor := consistencyFlavors()[0]
	shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
		enthnsw.NewDefaultUserConfig(), false, false)

	objs := consistencyObjects(flavor, className, count)
	require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))

	docIDs := make([]uint64, count)
	for i, obj := range objs {
		stored, err := shard.ObjectByID(t.Context(), obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		docIDs[i] = stored.DocID
	}

	var index VectorIndex
	_, err := shard.WithVectorIndex("", func(i VectorIndex) error {
		index = i
		return nil
	})
	require.NoError(t, err)
	underlyingShard(t, shard).setVectorIndex("", &ghostingIndex{VectorIndex: index, ghost: docIDs[0]})

	require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), newerVersions(objs, 0))))

	for i, obj := range objs {
		stored, err := shard.ObjectByID(t.Context(), obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		if i == 0 {
			assert.NotEqual(t, docIDs[i], stored.DocID, "the ghost has to get a new doc ID")
		} else {
			assert.Equal(t, docIDs[i], stored.DocID)
		}
	}
}

// TestRewriteKeepsDocIDOfUnchangedObject guards the optimization the repair sits
// next to: a resent object that needs no repair keeps its doc ID, whichever index
// answers the question.
func TestRewriteKeepsDocIDOfUnchangedObject(t *testing.T) {
	tests := []struct {
		name  string
		async bool
		vic   schemaConfig.VectorIndexConfig
	}{
		{name: "hnsw", vic: enthnsw.NewDefaultUserConfig()},
		{name: "flat", vic: flat.NewDefaultUserConfig()},
		{name: "index skipped", vic: enthnsw.UserConfig{Skip: true}},
		{name: "async indexing", async: true, vic: enthnsw.NewDefaultUserConfig()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			className := "RewriteDocID"
			flavor := consistencyFlavors()[0]
			shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className), test.vic,
				false, test.async)

			// enough objects for a graph with more than one level
			objs := consistencyObjects(flavor, className, 200)
			require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))

			before := make([]uint64, len(objs))
			for i, obj := range objs {
				stored, err := shard.ObjectByID(t.Context(), obj.ID(), nil, additional.Properties{})
				require.NoError(t, err)
				before[i] = stored.DocID
			}

			require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))

			for i, obj := range objs {
				stored, err := shard.ObjectByID(t.Context(), obj.ID(), nil, additional.Properties{})
				require.NoError(t, err)
				assert.Equal(t, before[i], stored.DocID)
			}
		})
	}
}

// TestConcurrentRewritesOfUnindexedObjects guards the repair against itself:
// every write sees the same missing vector and replaces the doc ID. Multi vectors
// are left out for https://github.com/weaviate/weaviate/issues/13686, which any
// concurrent update runs into, repair or not.
func TestConcurrentRewritesOfUnindexedObjects(t *testing.T) {
	const count, writers = 9, 8

	for _, flavor := range consistencyFlavors() {
		if flavor.target == "colbert" {
			continue
		}

		t.Run(flavor.name, func(t *testing.T) {
			className := "ConcurrentRewrite"
			shard, idx := testShardWithSettings(t, t.Context(), consistencyClass(className),
				enthnsw.NewDefaultUserConfig(), false, false, flavor.opts...)

			objs := consistencyObjects(flavor, className, count)
			for _, obj := range objs {
				storeWithoutVector(t, shard, obj)
			}

			eg := enterrors.NewErrorGroupWrapper(idx.logger)
			for range writers {
				batch := newerVersions(objs, 0)
				eg.Go(func() error {
					return errors.Join(shard.PutObjectBatch(t.Context(), batch)...)
				})
			}
			require.NoError(t, eg.Wait())

			requireSelfHits(t, shard, flavor, objs)
			cleanUpTombstones(t, shard, flavor)
			requireSelfHits(t, shard, flavor, objs)
		})
	}
}

// TestConcurrentUpdatesOfTheSameObjects is the natural trigger of issue #13609: an
// update deletes the doc ID another update is still inserting. Over rounds on a
// small shard that used to leave the shard at 0 or 1 results for any limit, all
// writes reported as successful. Multi vectors are left out for
// https://github.com/weaviate/weaviate/issues/13686.
func TestConcurrentUpdatesOfTheSameObjects(t *testing.T) {
	const count, writers, rounds = 9, 8, 30

	for _, flavor := range consistencyFlavors() {
		if flavor.target == "colbert" {
			continue
		}

		t.Run(flavor.name, func(t *testing.T) {
			for round := range rounds {
				className := "ConcurrentUpdates"
				shard, idx := testShardWithSettings(t, t.Context(), consistencyClass(className),
					enthnsw.NewDefaultUserConfig(), false, false, flavor.opts...)

				objs := consistencyObjects(flavor, className, count)
				require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs)))

				eg := enterrors.NewErrorGroupWrapper(idx.logger)
				for writer := range writers {
					batch := withNewVectors(flavor, objs, (writer+1)*count)
					eg.Go(func() error {
						return errors.Join(shard.PutObjectBatch(t.Context(), batch)...)
					})
				}
				require.NoError(t, eg.Wait())

				requireSelfHits(t, shard, flavor, objs)
				cleanUpTombstones(t, shard, flavor)
				requireSelfHits(t, shard, flavor, objs)
				if t.Failed() {
					t.Fatalf("round %d", round)
				}
			}
		})
	}
}

// noVectorQueues is a shard whose vector queues are gone, as they are mid-drop.
type noVectorQueues struct {
	ShardLike
}

func (s *noVectorQueues) WithVectorIndexQueue(string, func(*VectorIndexQueue) error) (bool, error) {
	return false, nil
}

// TestAsyncBatchReportsVectorErrorsOnTheirOwnObjects pins that a vector the queue
// refuses fails its own object. It used to fail the first object of the batch
// instead, and the objects behind the refused one were reported as written
// without ever reaching the queue.
func TestAsyncBatchReportsVectorErrorsOnTheirOwnObjects(t *testing.T) {
	const count, refused = 5, 2

	className := "AsyncVectorErrors"
	flavor := consistencyFlavors()[0]
	shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
		enthnsw.NewDefaultUserConfig(), false, true)

	objs := consistencyObjects(flavor, className, count)
	objs[refused].Vector = []float32{1, 2, 3}

	errs := shard.PutObjectBatch(t.Context(), objs)

	indexed := make([]*storobj.Object, 0, count-1)
	for i, obj := range objs {
		if i == refused {
			require.Errorf(t, errs[i], "object %d", i)
			continue
		}
		require.NoErrorf(t, errs[i], "object %d", i)
		indexed = append(indexed, obj)
	}

	require.Eventually(t, func() bool {
		for _, obj := range indexed {
			res, _, err := shard.ObjectVectorSearch(t.Context(), []models.Vector{obj.Vector}, []string{""},
				0, count, nil, nil, nil, additional.Properties{}, nil, nil)
			if err != nil || len(res) != len(indexed) {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond, "every accepted object has to reach the index")
}

func TestAsyncBatchReportsMissingQueueOnEveryObject(t *testing.T) {
	const count = 5

	className := "AsyncMissingQueue"
	flavor := consistencyFlavors()[0]
	shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
		enthnsw.NewDefaultUserConfig(), false, true)

	batcher := newObjectsBatcher(&noVectorQueues{ShardLike: shard}, shard.Index().logger)
	batcher.init(consistencyObjects(flavor, className, count))
	batcher.storeInObjectStore(t.Context())
	batcher.storeAdditionalStorageWithAsyncQueue(t.Context())

	for i, err := range batcher.errs {
		require.Errorf(t, err, "object %d", i)
	}
}

// TestSyncInsertRetriesWithoutUsableEntrypoint pins that a synchronous insert gets
// the retry the queue worker gives an asynchronous one. Without it the object
// stays in the LSM store while the insert is reported as failed.
func TestSyncInsertRetriesWithoutUsableEntrypoint(t *testing.T) {
	noEntrypoint := fmt.Errorf("pick entrypoint: %w", enterrors.ErrNoUsableEntrypoint)
	permanent := errors.New("commit log is gone")

	tests := []struct {
		name    string
		returns []error
		wantErr error
	}{
		{name: "first attempt works", returns: []error{nil}},
		{name: "clears on a retry", returns: []error{noEntrypoint, noEntrypoint, nil}},
		{
			name: "gives up eventually",
			returns: []error{
				noEntrypoint, noEntrypoint, noEntrypoint, noEntrypoint, noEntrypoint,
			},
			wantErr: enterrors.ErrNoUsableEntrypoint,
		},
		{name: "other errors are final", returns: []error{permanent}, wantErr: permanent},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := NewMockVectorIndex(t)
			for _, err := range test.returns {
				index.EXPECT().AddBatch(mock.Anything, mock.Anything, mock.Anything).Return(err).Once()
			}

			err := (&VectorIndexQueue{vectorIndex: index}).Insert(t.Context(),
				&common.Vector[[]float32]{ID: 1, Vector: consistencyVector(0)})

			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

func nonNilErrs(errs []error) []error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

func TestExperimentAsyncBatchErrors(t *testing.T) {
	className := "AsyncBlame"
	flavor := consistencyFlavors()[0]
	shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
		enthnsw.NewDefaultUserConfig(), false, true)

	seed := consistencyObjects(flavor, className, 1)
	require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), seed)))

	objs := consistencyObjects(flavor, className, 5)
	objs[2].Vector = make([]float32, 7)
	objs[2].Vector[0] = 1
	errs := shard.PutObjectBatch(t.Context(), objs)
	for i, err := range errs {
		ok, existsErr := shard.Exists(t.Context(), objs[i].ID())
		require.NoError(t, existsErr)
		t.Logf("object %d: stored=%v err=%v", i, ok, err)
	}
}
