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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const interleaveDeadline = 30 * time.Second

func runAsync(shard *Shard, fn func() error) <-chan error {
	done := make(chan error, 1)
	enterrors.GoWrapper(func() { done <- fn() }, shard.index.logger)
	return done
}

func awaitDone(t *testing.T, done <-chan error, format string, args ...any) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(interleaveDeadline):
		t.Fatalf(format, args...)
		return nil
	}
}

func isDone(done <-chan error) (bool, error) {
	select {
	case err := <-done:
		return true, err
	default:
		return false, nil
	}
}

// A write that loaded the unarmed state before an arm must still reach the migrated index.
func TestArmWaitsForWritesThatLoadedTheUnarmedState(t *testing.T) {
	ctx := testCtx()
	className := "ArmBarrier_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, "title")
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "alpha")))

	obj := createTestObjectWithText(className, "zulu")
	shard.asyncReplicationRWMux.RLock()
	var unlock sync.Once
	runlock := func() { unlock.Do(shard.asyncReplicationRWMux.RUnlock) }
	defer runlock()
	status, err := shard.determineInsertStatus(nil, obj)
	require.NoError(t, err)
	obj.DocID = status.docID
	w, err := shard.prepareInvertedWrite(obj, nil, status)
	require.NoError(t, err)
	defer w.release()

	task, _ := newEnableFilterableTask(t, idx, className, shard.migrationUnit(), "title")
	done := runAsync(shard, func() error { return task.RunReindexOnlyOnShard(ctx, shard) })
	var reindexErr error
	finished := false
	require.Eventually(t, func() bool {
		if finished, reindexErr = isDone(done); finished {
			return true
		}
		if shard.asyncReplicationRWMux.TryRLock() {
			shard.asyncReplicationRWMux.RUnlock()
			return false
		}
		return true // TryRLock fails only once the arm's Lock is queued behind the RLock this test holds
	}, interleaveDeadline, time.Millisecond)

	idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
	require.NoError(t, err)
	objBinary, err := obj.MarshalBinaryDisk(false)
	require.NoError(t, err)
	require.NoError(t, shard.upsertObjectDataLSM(shard.store.Bucket(helpers.ObjectsBucketLSM), idBytes, objBinary, obj.DocID))
	require.NoError(t, w.apply())
	w.release()
	runlock()

	if !finished {
		reindexErr = <-done
	}
	require.NoError(t, reindexErr)
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, filterableHas(t, shard, "title", "zulu", obj.DocID),
		"the object written while the migration was being armed is missing from the migrated index")
}

// A swap's teardown waits for a write that pinned the old bucket, so the write reaches the bucket that replaces it.
func TestSwapTeardownWaitsForPinnedWrites(t *testing.T) {
	long := make([]string, 1000)
	for i := range long {
		long[i] = "w" + strings.Repeat("x", 4) + uuid.NewString()[:4]
	}
	for _, tc := range []struct {
		name, value string
		idle        bool // true flushes the old bucket first, so the pinned write opens a new commit log
	}{
		{"idle old bucket", "gamma", true},
		{"more than 4 KB of terms into a pending commit log", strings.Join(long, " "), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SwapPins_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			prev := createTestObjectWithText(className, "alpha")
			require.NoError(t, shard.PutObject(ctx, prev))
			task, _ := newFilterableRetokenizeTask(t, idx, className, "title",
				models.PropertyTokenizationField, shard.migrationUnit())
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			staged := shard.store.Bucket(task.ingestBucketName("title"))
			require.NotNil(t, staged)
			if tc.idle {
				require.NoError(t, shard.store.Bucket(helpers.BucketFromPropNameLSM("title")).FlushAndSwitch())
			}

			idBytes, err := uuid.MustParse(prev.ID().String()).MarshalBinary()
			require.NoError(t, err)
			stored, err := fetchObject(shard.store.Bucket(helpers.ObjectsBucketLSM), idBytes)
			require.NoError(t, err)
			next := createTestObjectWithText(className, tc.value)
			next.Object.ID = prev.ID()
			status, err := shard.determineInsertStatus(stored, next)
			require.NoError(t, err)
			next.DocID = status.docID
			w, err := shard.prepareInvertedWrite(next, stored, status)
			require.NoError(t, err)
			defer w.release()

			done := runAsync(shard, func() error { return task.RunSwapOnShard(ctx, shard) })
			require.Eventually(t, func() bool {
				return shard.store.Bucket(helpers.BucketFromPropNameLSM("title")) == staged
			}, interleaveDeadline, time.Millisecond)
			finished, _ := isDone(done)
			require.False(t, finished, "the swap must wait for the pinned write")

			require.NoError(t, w.apply())
			w.release()
			require.NoError(t, <-done)
			require.True(t, filterableHas(t, shard, "title", tc.value, next.DocID),
				"the write that pinned the old bucket is missing from the new one")
		})
	}
}

// Every write path (put, merge, both deletes, reference batch) releases its bucket pins when it fails.
func TestWriteReleasesPinsOnEveryExit(t *testing.T) {
	enableHashtree := func(t *testing.T, shard *Shard) {
		ht, err := hashtree.NewHashTree(defaultHashtreeHeightSingleTenant)
		require.NoError(t, err)
		shard.asyncReplicationRWMux.Lock()
		shard.hashtree, shard.asyncReplicationCancelFunc = ht, func() {}
		shard.asyncReplicationRWMux.Unlock()
	}
	putAt := func(t *testing.T, shard *Shard, className string, updateTime int64) *storobj.Object {
		obj := createTestObjectWithText(className, "alpha")
		obj.Object.LastUpdateTimeUnix = updateTime
		require.NoError(t, shard.PutObject(testCtx(), obj))
		return obj
	}

	tests := []struct {
		name   string
		pinned string
		write  func(t *testing.T, shard *Shard, className string) error
	}{
		{"put", helpers.BucketFromPropNameLSM("title"), func(t *testing.T, shard *Shard, className string) error {
			enableHashtree(t, shard)
			return shard.PutObject(testCtx(), createTestObjectWithText(className, "alpha"))
		}},
		{"merge", helpers.BucketFromPropNameLSM("title"), func(t *testing.T, shard *Shard, className string) error {
			obj := putAt(t, shard, className, time.Now().UnixMilli())
			enableHashtree(t, shard)
			return shard.MergeObject(testCtx(), objects.MergeDocument{
				Class: className, ID: obj.ID(), PrimitiveSchema: map[string]interface{}{"title": "beta"},
			})
		}},
		{"delete", helpers.BucketFromPropNameLSM("title"), func(t *testing.T, shard *Shard, className string) error {
			obj := putAt(t, shard, className, 0)
			enableHashtree(t, shard)
			return shard.DeleteObject(testCtx(), obj.ID(), time.Time{})
		}},
		{"batch delete", helpers.BucketFromPropNameLSM("title"), func(t *testing.T, shard *Shard, className string) error {
			obj := putAt(t, shard, className, 0)
			enableHashtree(t, shard)
			return shard.DeleteObjectBatch(testCtx(), []strfmt.UUID{obj.ID()}, time.Time{}, false)[0].Err
		}},
		{"reference batch", helpers.BucketFromPropNameLSM("ref"), func(t *testing.T, shard *Shard, className string) error {
			target := putAt(t, shard, className, time.Now().UnixMilli())
			return shard.AddReferencesBatch(testCtx(), objects.BatchReferences{{
				From: crossref.NewSource(schema.ClassName(className), "ref", strfmt.UUID(uuid.NewString())),
				To:   crossref.New("localhost", className, target.ID()),
			}})[0]
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PinRelease_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			class.Properties = append(class.Properties, &models.Property{Name: "ref", DataType: []string{className}})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			// A leaked pin would hang Shutdown.
			defer func() {
				if !t.Failed() {
					shard.Shutdown(ctx)
				}
			}()

			require.ErrorContains(t, tt.write(t, shard, className), map[string]string{
				"put": "last update time", "merge": "last update time",
				"delete": "invalid object update time", "batch delete": "invalid object update time",
				"reference batch": "not found",
			}[tt.name])
			done := runAsync(shard, func() error { return shard.store.ShutdownBucket(ctx, tt.pinned) })
			require.NoError(t, awaitDone(t, done, "shutting down %q hangs on a pin the failed %s never released", tt.pinned, tt.name))
		})
	}
}

// blockingDeleteIndex parks a vector-index delete, the way a delete waits behind an HNSW compression.
type blockingDeleteIndex struct {
	VectorIndex
	entered, unblock chan struct{}
}

func (b *blockingDeleteIndex) Delete(ids ...uint64) error {
	close(b.entered)
	<-b.unblock
	return b.VectorIndex.Delete(ids...)
}

// A delete waiting in its vector-index step must not hold the inverted-index pins
// that a property's ShutdownBucket waits on.
func TestDeleteReleasesPinsBeforeVectorIndexWork(t *testing.T) {
	for _, w := range missingBucketWrites {
		if !w.deletes {
			continue
		}
		t.Run(w.name, func(t *testing.T) {
			ctx := testCtx()
			className := "DeleteVectorStall_" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			obj := createTestObjectWithText(className, "alpha")
			require.NoError(t, shard.PutObject(ctx, obj))

			queue, ok := shard.GetVectorIndexQueue("")
			require.True(t, ok)
			stall := &blockingDeleteIndex{VectorIndex: queue.vectorIndex, entered: make(chan struct{}), unblock: make(chan struct{})}
			queue.vectorIndex = stall
			var unblock sync.Once
			defer unblock.Do(func() { close(stall.unblock) })

			deleted := runAsync(shard, func() error { return w.write(ctx, shard, obj) })
			select {
			case <-stall.entered:
			case err := <-deleted:
				t.Fatalf("the delete finished without reaching the vector index: %v", err)
			case <-time.After(interleaveDeadline):
				t.Fatalf("the %s never reached the vector index", w.name)
			}
			shutdown := runAsync(shard, func() error {
				return shard.store.ShutdownBucket(ctx, helpers.BucketFromPropNameLSM("title"))
			})
			require.NoError(t, awaitDone(t, shutdown, "ShutdownBucket waits on pins the %s holds through its vector-index delete", w.name))
			unblock.Do(func() { close(stall.unblock) })
			require.NoError(t, awaitDone(t, deleted, "the %s never finished", w.name))
		})
	}
}

// WriteWALs takes the flushLock ReplaceBuckets holds on the replacement while draining the old bucket's pins.
func TestDeleteReleasesPinsBeforeWriteWALs(t *testing.T) {
	ctx := testCtx()
	className := "DeleteWALReplace_" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer func() {
		if !t.Failed() {
			shard.Shutdown(ctx)
		}
	}()
	obj := createTestObjectWithText(className, "alpha")
	require.NoError(t, shard.PutObject(ctx, obj))
	title := helpers.BucketFromPropNameLSM("title")
	replacement := title + "_replacement"
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, replacement, shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...))

	// The mirror delete runs last in apply, with title pinned, so the replace starts and swaps the name there.
	replaced := make(chan error, 1)
	var once sync.Once
	mirrorDelete := func(*Shard, uint64, *inverted.Property) error {
		once.Do(func() {
			old := shard.store.Bucket(title)
			enterrors.GoWrapper(func() { replaced <- shard.store.ReplaceBuckets(ctx, title, replacement) }, shard.index.logger)
			for deadline := time.Now().Add(interleaveDeadline); shard.store.Bucket(title) == old && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
			}
		})
		return nil
	}
	noop := func(*Shard, uint64, *inverted.Property) error { return nil }
	shard.registerDoubleWriteWithScope([]string{"title"}, nil,
		func(map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
			return noop, mirrorDelete
		})

	deleted := runAsync(shard, func() error { return shard.DeleteObject(ctx, obj.ID(), time.Time{}) })
	require.NoError(t, awaitDone(t, deleted, "the delete deadlocks in WriteWALs against ReplaceBuckets"))
	require.NoError(t, awaitDone(t, replaced, "ReplaceBuckets never finished"))
}

// A failed mirror analysis still lets the main index change and the previous value's mirror delete run.
func TestMirrorAnalysisErrorKeepsMainChangesAndMirrorDelete(t *testing.T) {
	for _, tc := range []struct {
		name      string
		prev      string
		next      interface{} // nil deletes
		mirrorDel string      // value the mirror must still delete; empty for none
	}{
		{"delete of an object the overlay rejects", "", nil, ""},
		{"update to a value the overlay rejects", "abc", 123.0, "abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MirrorAnalysisError_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title", "plain"})
			class.Properties[1].IndexFilterable, class.Properties[1].IndexSearchable = boolPtr(false), boolPtr(false)
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			obj := createTestObjectWithText(className, "alpha")
			var prev interface{} = 123.0
			if tc.prev != "" {
				prev = tc.prev
			}
			obj.Object.Properties.(map[string]interface{})["plain"] = prev
			require.NoError(t, shard.PutObject(ctx, obj))
			stored, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
			require.NoError(t, err)

			var mirrorDeleted []string
			record := func(_ *Shard, _ uint64, p *inverted.Property) error {
				for _, item := range p.Items {
					mirrorDeleted = append(mirrorDeleted, string(item.Data))
				}
				return nil
			}
			noop := func(*Shard, uint64, *inverted.Property) error { return nil }
			shard.registerDoubleWriteWithScope([]string{"plain"},
				map[string]inverted.PropertyOverlay{"plain": {ForceFilterable: true}},
				func(map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
					return noop, record
				})

			if tc.next == nil {
				err = shard.DeleteObject(ctx, obj.ID(), time.Time{})
			} else {
				next := createTestObjectWithText(className, "beta")
				next.Object.ID = obj.ID()
				next.Object.Properties.(map[string]interface{})["plain"] = tc.next
				err = shard.PutObject(ctx, next)
			}
			require.ErrorContains(t, err, "migration double-write")
			require.False(t, filterableHas(t, shard, "title", "alpha", stored.DocID), "the main index change must still run")
			if tc.mirrorDel != "" {
				require.Contains(t, mirrorDeleted, tc.mirrorDel, "the mirror delete of the previous value must still run")
			}
		})
	}
}
