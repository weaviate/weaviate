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
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

// loadTestShard returns the single-shard test index's shard, loaded, by name
// and as the concrete instance the teardown paths operate on.
func loadTestShard(t *testing.T, index *Index) (string, *Shard) {
	t.Helper()

	var name string
	index.shards.Range(func(n string, _ ShardLike) error {
		name = n
		return nil
	})
	entry := index.shards.Load(name)
	require.NotNil(t, entry)
	require.NoError(t, entry.(*LazyLoadShard).Load(context.Background()))
	return name, entry.(*LazyLoadShard).shard
}

// dropTestShard pins the shard as Index.withShardForWrite does, then races a
// drop() against it. Returns once that drop is inside its drain.
func dropTestShard(t *testing.T, index *Index) (shard *Shard, release func(), dropped <-chan error) {
	t.Helper()

	name, shard := loadTestShard(t, index)

	_, release, err := index.GetShard(context.Background(), name)
	require.NoError(t, err)

	// keeps cleanup() from retrying Shutdown on the dropped shard
	t.Cleanup(func() { index.shards.LoadAndDelete(name) })

	ch := make(chan error, 1)
	go func() { ch <- shard.drop(false) }()

	// the flag is drainRefsForDrop's first action
	require.Eventually(t, shard.dropRequested.Load, 10*time.Second, time.Millisecond,
		"drop never entered its drain")

	return shard, release, ch
}

func requireDropped(t *testing.T, dropped <-chan error) {
	t.Helper()
	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("drop never completed")
	}
}

func dropTestObject(vec []float32) (*storobj.Object, []byte) {
	id := strfmt.UUID(uuid.New().String())
	idBytes, _ := uuid.MustParse(id.String()).MarshalBinary()
	return storobj.FromObject(&models.Object{
		Class: "Test", ID: id, Properties: map[string]interface{}{},
	}, vec, nil, nil), idBytes
}

// TestShardDropWaitsForInFlightReferences pins the race that crashed nodes:
// drop tore the store down while a pinned write was mid-flight, so
// Store.Bucket(objects) went nil under it.
func TestShardDropWaitsForInFlightReferences(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	shard, release, dropped := dropTestShard(t, index)

	select {
	case err := <-dropped:
		t.Fatalf("drop completed while a write still held a reference: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	// no new writer may sneak in behind the drain either
	_, err := shard.preventShutdown()
	require.ErrorIs(t, err, errDropInProgress)

	release()
	requireDropped(t, dropped)
}

// TestShardDropDrainsRealBatchWrite is the end-to-end form: every object must
// succeed, not merely fail cleanly. Asserting success is what keeps the drain
// responsible for this case rather than objectsBucket's backstop.
func TestShardDropDrainsRealBatchWrite(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	batch := make([]*storobj.Object, 50)
	for i := range batch {
		batch[i], _ = dropTestObject([]float32{float32(i), 1, 2, 3})
	}

	shard, release, dropped := dropTestShard(t, index)

	errs := func() []error {
		defer release()
		return shard.PutObjectBatch(context.Background(), batch)
	}()
	for i, err := range errs {
		require.NoErrorf(t, err, "object %d of a batch in flight during drop", i)
	}

	requireDropped(t, dropped)
}

// TestShardDropProceedsWhenDrainTimesOut pins the escape hatch: the drain is
// bounded on purpose, so a reference held past the window must not wedge the
// delete, and must be logged. Runs for the full drain window (~30s).
func TestShardDropProceedsWhenDrainTimesOut(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	logger, hook := test.NewNullLogger()
	index.logger = logger

	start := time.Now()
	_, release, dropped := dropTestShard(t, index) // pin is never released
	defer release()

	requireDropped(t, dropped)
	// ~30s window; near-instant means it never waited
	require.Greater(t, time.Since(start), 10*time.Second, "drop gave up well short of the drain window")

	var warned bool
	for _, e := range hook.AllEntries() {
		warned = warned || (e.Level == logrus.ErrorLevel &&
			strings.Contains(e.Message, "proceeding with drop while references are still held"))
	}
	require.True(t, warned, "a drop that outran its drain must be logged, not silent")
}

// TestObjectReadsAfterStoreTeardownReturnErrors is the read-side sibling: a
// query outliving the drain reads the objects bucket through the same
// deregistered-bucket window. The bucket view has no error to return, so it
// yields the zero view and the reads taken against it fail individually.
func TestObjectReadsAfterStoreTeardownReturnErrors(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	_, shard := loadTestShard(t, index)
	require.NoError(t, shard.store.Shutdown(context.Background()))

	obj, _ := dropTestObject(nil)
	id := obj.ID()

	reads := map[string]func() error{
		"exists": func() error {
			_, err := shard.Exists(context.Background(), id)
			return err
		},
		"object digest": func() error {
			_, err := shard.ObjectDigestErrDeleted(context.Background(), id)
			return err
		},
		"object digests in range": func() error {
			end := strfmt.UUID(uuid.Max.String())
			_, err := shard.ObjectDigestsInRange(context.Background(), id, end, 10)
			return err
		},
		"vector by doc id": func() error {
			_, err := shard.vectorByIndexID(context.Background(), 0, "")
			return err
		},
		"multi vector by doc id": func() error {
			_, err := shard.multiVectorByIndexID(context.Background(), 0, "")
			return err
		},
		"object by id": func() error {
			_, err := shard.ObjectByID(context.Background(), id, nil, additional.Properties{})
			return err
		},
		"multi object by id": func() error {
			_, err := shard.MultiObjectByID(context.Background(), []multi.Identifier{{ID: id.String()}})
			return err
		},
		"multi object raw by id": func() error {
			_, err := shard.MultiObjectRawByID(context.Background(), []strfmt.UUID{id})
			return err
		},
		"object digests": func() error {
			_, err := shard.ObjectDigests(context.Background(), []multi.Identifier{{ID: id.String()}})
			return err
		},
		"compare digests": func() error {
			_, err := shard.CompareDigests(context.Background(),
				[]types.RepairDigest{{ID: uuid.MustParse(id.String())}})
			return err
		},
		"object by doc id with props": func() error {
			_, err := shard.objectByIndexIDWithProps(context.Background(), 0, nil)
			return err
		},
		"uuid from doc id": func() error {
			_, err := shard.uuidFromDocID(0)
			return err
		},
		"object list": func() error {
			_, err := shard.ObjectList(context.Background(), 10, nil, nil, additional.Properties{},
				shard.index.Config.ClassName)
			return err
		},
		"cursor object list": func() error {
			_, err := shard.cursorObjectList(context.Background(), &filters.Cursor{Limit: 10},
				additional.Properties{}, shard.index.Config.ClassName)
			return err
		},
		"was deleted": func() error {
			_, _, err := shard.WasDeleted(context.Background(), id)
			return err
		},
		"object vector search": func() error {
			_, _, err := shard.ObjectVectorSearch(context.Background(),
				[]models.Vector{[]float32{1, 2, 3, 4}}, []string{""}, 0, 10, nil, nil, nil,
				additional.Properties{}, nil, nil)
			return err
		},
	}

	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, read(), lsmkv.ErrBucketNotFound)
		})
	}

	t.Run("releasing the zero bucket view", func(t *testing.T) {
		require.NotPanics(t, func() { shard.GetObjectsBucketView().ReleaseView() })
	})
}

// TestObjectWritesAfterStoreTeardownReturnErrors covers the backstop: a write
// outliving the drain must fail on the deregistered bucket rather than
// dereference nil, and must be reported once.
func TestObjectWritesAfterStoreTeardownReturnErrors(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	logger, hook := test.NewNullLogger()
	index.logger = logger

	_, shard := loadTestShard(t, index)
	// the state a teardown that outran its drain leaves behind
	require.NoError(t, shard.store.Shutdown(context.Background()))

	obj, idBytes := dropTestObject(nil)
	id := obj.ID()
	merge := objects.MergeDocument{Class: "Test", ID: id}

	mutations := map[string]func() error{
		"put": func() error {
			_, err := shard.putObjectLSM(context.Background(), obj, idBytes)
			return err
		},
		"delete": func() error {
			_, err := shard.deleteObject(context.Background(), id, time.Now(), false)
			return err
		},
		"batch delete": func() error {
			return shard.batchDeleteObject(context.Background(), id, time.Now())
		},
		"merge": func() error {
			_, _, err := shard.mergeObjectInStorage(context.Background(), merge, idBytes)
			return err
		},
		"mutable merge": func() error {
			_, err := shard.mutableMergeObjectLSM(context.Background(), merge, idBytes)
			return err
		},
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, mutate(), lsmkv.ErrBucketNotFound)
		})
	}

	var reports int
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "request reached a torn-down store") {
			reports++
		}
	}
	require.Equal(t, 1, reports, "an outrun drain must be reported exactly once per shard")
}
