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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestRebuildShardsBeforeLoad(t *testing.T) {
	// the objects of the recount tests, each with a vector of 3 dimensions
	const wantDims = 2 * 3

	type outcomes struct{ rebuilt, failed int }
	tests := []struct {
		name             string
		reindex, recount bool
		// breakRebuild makes a rebuild fail, and restores what the others need.
		breakRebuild func(t *testing.T, shard *Shard, schemaGetter *fakeSchemaGetter) (restore func())
		// keepDimensions keeps the dimensions bucket, which is otherwise lost before the restart.
		keepDimensions bool
		// wantAnalyzed is the number of objects the recount analyzes with both enabled.
		wantAnalyzed int
		wantDims     int
		wantTally    []int
		wantReindex  outcomes
		wantRecount  outcomes
	}{
		{
			name:        "dimensions reindex only",
			reindex:     true,
			wantDims:    wantDims,
			wantTally:   []int{5 + 100, 2 + 1},
			wantReindex: outcomes{rebuilt: 1},
		},
		{
			name:        "property lengths recount only",
			recount:     true,
			wantTally:   []int{5, 2},
			wantRecount: outcomes{rebuilt: 1},
		},
		{
			name:    "both",
			reindex: true, recount: true,
			wantAnalyzed: 2,
			wantDims:     wantDims,
			wantTally:    []int{5, 2},
			wantReindex:  outcomes{rebuilt: 1},
			wantRecount:  outcomes{rebuilt: 1},
		},
		{
			name:    "both, recount cannot start",
			reindex: true, recount: true,
			breakRebuild: func(t *testing.T, shard *Shard, _ *fakeSchemaGetter) func() {
				// a non-empty directory cannot be removed as a leftover recount file
				blocker := filepath.Join(shard.path(), propLengthsRecountFile, "blocker")
				require.NoError(t, os.MkdirAll(blocker, 0o755))
				return func() {}
			},
			wantDims:    wantDims,
			wantTally:   []int{5 + 100, 2 + 1},
			wantReindex: outcomes{rebuilt: 1},
			wantRecount: outcomes{failed: 1},
		},
		{
			name:    "both, recount fails during the walk",
			reindex: true, recount: true,
			wantAnalyzed: 1,
			breakRebuild: func(_ *testing.T, _ *Shard, schemaGetter *fakeSchemaGetter) func() {
				previous := schemaGetter.schema
				schemaGetter.schema = schema.Schema{Objects: &models.Schema{}}
				return func() { schemaGetter.schema = previous }
			},
			wantDims:    wantDims,
			wantTally:   []int{5 + 100, 2 + 1},
			wantReindex: outcomes{rebuilt: 1},
			wantRecount: outcomes{failed: 1},
		},
		{
			name:    "both, dimensions reindex fails",
			reindex: true, recount: true,
			breakRebuild: func(t *testing.T, shard *Shard, _ *fakeSchemaGetter) func() {
				// a leftover replacement bucket that cannot be removed fails the switch
				stuck := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM+"__to_roaringset_ready", "stuck")
				require.NoError(t, os.MkdirAll(stuck, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(stuck, "file"), []byte("x"), 0o600))
				require.NoError(t, os.Chmod(stuck, 0o500))
				return func() { _ = os.Chmod(stuck, 0o700) }
			},
			keepDimensions: true,
			wantAnalyzed:   2,
			wantDims:       wantDims,
			wantTally:      []int{5, 2},
			wantReindex:    outcomes{failed: 1},
			wantRecount:    outcomes{rebuilt: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, schemaGetter := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
			defer repo.Shutdown(context.Background())
			for _, name := range recountTestNames {
				require.NoError(t, repo.PutObject(testCtx(), &models.Object{
					Class:      recountTestClass,
					ID:         strfmt.UUID(uuid.NewString()),
					Properties: map[string]any{"name": name},
				}, []float32{1, 2, 3}, nil, nil, nil, 0))
			}
			shard := recountTestShard(t, repo)
			idx, name, class := shard.index, shard.name, shard.class
			trackerPath := shard.GetPropertyLengthTracker().FileName()
			require.NoError(t, shard.Shutdown(testCtx()))
			idx.shards.LoadAndDelete(name)
			inflateTracker(t, trackerPath)
			if !tt.keepDimensions {
				require.NoError(t, os.RemoveAll(filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)))
			}
			if tt.breakRebuild != nil {
				defer tt.breakRebuild(t, shard, schemaGetter)()
			}

			// The recount analyzes each object it is fed, and the reindex holds the
			// dimensions bucket of the shard from before its walk until after it.
			var analyzed, walkedApart int
			idx.getSchema = &recountHookSchemaGetter{SchemaGetter: schemaGetter, onReadOnlyClass: func() {
				if _, err := os.Stat(recountFileOf(trackerPath)); err != nil || !tt.reindex {
					return
				}
				analyzed++
				ctx, cancel := context.WithTimeout(testCtx(), 50*time.Millisecond)
				defer cancel()
				if unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, idx.path(), name); err == nil {
					unlock()
					walkedApart++
				}
			}}
			reindex := &startupRebuild{enabled: tt.reindex}
			recount := &startupRebuild{enabled: tt.recount}
			idx.Config.DimensionsReindex, idx.Config.PropertyLengthsRecount = reindex, recount

			idx.rebuildShardsBeforeLoad(testCtx(), class, []string{name})

			assert.Equal(t, tt.wantAnalyzed, analyzed)
			assert.Zero(t, walkedApart, "the recount must be fed by the walk of the reindex")
			sum, count := diskTally(t, trackerPath)
			assert.Equal(t, tt.wantTally, []int{sum, count})
			usage, err := shardusage.CalculateUnloadedDimensionsUsage(testCtx(), repo.logger, idx.path(), name, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantDims, usage.Count*usage.Dimensions)
			rebuilt, failed, _ := reindex.summary()
			assert.Equal(t, tt.wantReindex, outcomes{rebuilt, failed}, "dimensions reindex")
			rebuilt, failed, _ = recount.summary()
			assert.Equal(t, tt.wantRecount, outcomes{rebuilt, failed}, "property lengths recount")
		})
	}
}

// walkTestVisitor keeps the objects it is fed.
type walkTestVisitor struct {
	decode       additional.Properties
	failAt       int
	panicAt      int
	objects      []*storobj.Object
	propsSeen    int
	vectorsSeen  int
	multiSeen    int
	vectorLenSum int
}

func (v *walkTestVisitor) decodeOptions() additional.Properties {
	return v.decode
}

func (v *walkTestVisitor) visit(object *storobj.Object) error {
	v.objects = append(v.objects, object)
	switch len(v.objects) {
	case v.failAt:
		return errors.New("visitor failed")
	case v.panicAt:
		panic("visitor panicked")
	}
	if object.Properties() != nil {
		v.propsSeen++
	}
	if len(object.Vectors["named"]) > 0 {
		v.vectorsSeen++
	}
	if len(object.MultiVectors["multi"]) > 0 {
		v.multiSeen++
	}
	v.vectorLenSum += object.VectorLen
	return nil
}

func TestShard_WalkObjects(t *testing.T) {
	const objects = 5
	readsProps := func() *walkTestVisitor { return &walkTestVisitor{} }
	readsVectors := func() *walkTestVisitor {
		return &walkTestVisitor{decode: additional.Properties{NoProps: true, Vectors: []string{"named", "multi"}}}
	}

	tests := []struct {
		name     string
		visitors func() []*walkTestVisitor
		// corrupt makes the properties of the first object walked undecodable.
		corrupt bool
		cancel  bool
		// wantFed is the number of objects each visitor is fed, wantErr whether it fails.
		wantFed []int
		wantErr []bool
	}{
		{
			name:     "each visitor reads what it asks for",
			visitors: func() []*walkTestVisitor { return []*walkTestVisitor{readsProps(), readsVectors()} },
			wantFed:  []int{objects, objects},
			wantErr:  []bool{false, false},
		},
		{
			name: "a visitor failing does not stop the others",
			visitors: func() []*walkTestVisitor {
				failing := readsProps()
				failing.failAt = 2
				return []*walkTestVisitor{failing, readsVectors()}
			},
			wantFed: []int{2, objects},
			wantErr: []bool{true, false},
		},
		{
			name: "a visitor panicking does not stop the others",
			visitors: func() []*walkTestVisitor {
				panicking := readsVectors()
				panicking.panicAt = 3
				return []*walkTestVisitor{readsProps(), panicking}
			},
			wantFed: []int{objects, 3},
			wantErr: []bool{false, true},
		},
		{
			name:     "an object that does not decode fails only the visitor reading what is broken",
			visitors: func() []*walkTestVisitor { return []*walkTestVisitor{readsProps(), readsVectors()} },
			corrupt:  true,
			wantFed:  []int{0, objects},
			wantErr:  []bool{true, false},
		},
		{
			name:     "a cancelled walk fails every visitor",
			visitors: func() []*walkTestVisitor { return []*walkTestVisitor{readsProps(), readsVectors()} },
			cancel:   true,
			wantFed:  []int{0, 0},
			wantErr:  []bool{true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shard, _, class := recalculationTestShard(t, ctx)
			defer shard.Shutdown(ctx)
			for range objects {
				obj := testObject(class.Class)
				obj.Vector = randVector(3)
				obj.Vectors = map[string][]float32{"named": randVector(5)}
				obj.MultiVectors = map[string][][]float32{"multi": {randVector(4), randVector(4)}}
				obj.Object.Properties = map[string]any{"text": "some text"}
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			if tt.corrupt {
				objectsBucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
				cursor := objectsBucket.Cursor()
				key, data := cursor.First()
				key, data = bytes.Clone(key), bytes.Clone(data)
				cursor.Close()
				broken := bytes.Replace(data, []byte(`"some text"}`), []byte(`"some text"!`), 1)
				require.NotEqual(t, data, broken)
				// the doc id follows the version byte
				docID := data[1:9]
				require.NoError(t, objectsBucket.Put(key, broken,
					lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docID)))
			}
			walkCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tt.cancel {
				cancel()
			}

			visitors := tt.visitors()
			asVisitors := make([]objectsVisitor, len(visitors))
			for i, v := range visitors {
				asVisitors[i] = v
			}
			walked, errs := shard.walkObjects(walkCtx, asVisitors...)

			if !tt.cancel {
				assert.Equal(t, objects, walked)
			}
			for i, v := range visitors {
				assert.Equal(t, tt.wantErr[i], errs[i] != nil, "visitor %d: %v", i, errs[i])
				assert.Len(t, v.objects, tt.wantFed[i], "visitor %d", i)
				if tt.wantErr[i] {
					continue
				}
				if v.decode.NoProps {
					assert.Equal(t, objects, v.vectorsSeen, "named vectors read by visitor %d", i)
					assert.Equal(t, objects, v.multiSeen, "multi vectors read by visitor %d", i)
				} else {
					assert.Equal(t, objects, v.propsSeen, "properties read by visitor %d", i)
				}
				assert.Equal(t, 3*objects, v.vectorLenSum, "legacy vector length read by visitor %d", i)
			}
			if !tt.corrupt && !tt.cancel {
				for i := 0; i < min(len(visitors[0].objects), len(visitors[1].objects)); i++ {
					assert.Same(t, visitors[0].objects[i], visitors[1].objects[i], "object %d must be decoded once", i)
				}
			}
		})
	}
}
