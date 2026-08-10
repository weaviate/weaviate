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
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// loadTestShard returns the single-shard test index's shard, loaded, as both
// the map key and the concrete instance the teardown paths operate on.
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

// dropTestShard pins the shard — the same pin Index.withShardForWrite holds
// around a write — and starts a drop() racing it in the background.
func dropTestShard(t *testing.T, index *Index) (shard *Shard, release func(), dropped <-chan error) {
	t.Helper()

	name, shard := loadTestShard(t, index)

	_, release, err := index.GetShard(context.Background(), name)
	require.NoError(t, err)

	// keeps cleanup() from retrying Shutdown on the dropped shard
	t.Cleanup(func() { index.shards.LoadAndDelete(name) })

	ch := make(chan error, 1)
	go func() { ch <- shard.drop(false) }()
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
// Store.Bucket(objects) went nil under it (nil *Bucket -> Get ->
// GetConsistentView -> SIGSEGV).
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

// TestShardDropDrainsRealBatchWrite is the end-to-end form. The write paths
// dereference Store.Bucket unguarded, so the drain is the only thing between
// this batch and a SIGSEGV — every object must succeed, not merely fail
// cleanly.
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
// delete — and must be logged, since that line is the only warning preceding
// the crash TestUnguardedWriteAfterTeardownCrashesProcess documents. Runs for
// the full drain window (~30s).
func TestShardDropProceedsWhenDrainTimesOut(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	logger, hook := test.NewNullLogger()
	index.logger = logger

	start := time.Now()
	_, release, dropped := dropTestShard(t, index) // pin is never released
	defer release()

	requireDropped(t, dropped)
	// the drain window is ~30s; anything near-instant means it never waited
	require.Greater(t, time.Since(start), 10*time.Second, "drop gave up well short of the drain window")

	var warned bool
	for _, e := range hook.AllEntries() {
		warned = warned || (e.Level == logrus.ErrorLevel &&
			strings.Contains(e.Message, "proceeding with drop while references are still held"))
	}
	require.True(t, warned, "a drop that outran its drain must be logged, not silent")
}

// TestUnguardedWriteAfterTeardownCrashesProcess pins the known-bad path left
// open on purpose: the write paths dereference Store.Bucket unguarded, so a
// write outliving a teardown drain kills the node instead of failing the
// request. The drain keeps it unreachable in practice; this makes the trade
// visible in code. Subprocess, because the failure is a process-level SIGSEGV.
//
// Reintroducing a nil guard on the write path fails this test — delete it in
// that same change.
func TestUnguardedWriteAfterTeardownCrashesProcess(t *testing.T) {
	if os.Getenv("WEAVIATE_TEST_TORN_STORE_CHILD") == "1" {
		index, cleanup := initIndexAndPopulate(t, t.TempDir())
		defer cleanup()

		_, shard := loadTestShard(t, index)
		require.NoError(t, shard.store.Shutdown(context.Background()))

		obj, idBytes := dropTestObject(nil)
		_, _ = shard.putObjectLSM(context.Background(), obj, idBytes)
		t.Fatal("write against a torn-down store returned instead of crashing")
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestUnguardedWriteAfterTeardownCrashesProcess")
	cmd.Env = append(os.Environ(), "WEAVIATE_TEST_TORN_STORE_CHILD=1")
	out, err := cmd.CombinedOutput()

	require.Errorf(t, err, "child survived a write against a torn-down store:\n%s", out)
	require.Contains(t, string(out), "nil pointer dereference")
	require.Contains(t, string(out), "GetConsistentView")
}
