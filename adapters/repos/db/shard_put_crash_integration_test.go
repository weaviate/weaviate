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
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A put stores the object in the store, gives its vector to the vector index,
// and only then has the store write its buffered logs (putOne: putObjectLSM,
// updateVectorIndex, store.WriteWALs). But updateVectorIndex flushes the
// vector index itself, or the queue of an asynchronous one, before it
// returns: what the index logged of the vector reaches the system before the
// log of the object does. A process that dies between the two leaves the
// vector on disk and not its object. The object and its vector should come
// back after the restart both or neither.
//
// The batch path writes the logs of the store first and flushes the queues
// after (objectsBatcher.flushWALs), and does not leave this.
func TestShard_CrashBetweenTheVectorIndexAndTheLogOfTheObject(t *testing.T) {
	for _, tt := range []struct {
		name  string
		vic   schemaConfig.VectorIndexConfig
		async bool
	}{
		{name: "hnsw", vic: hnswent.NewDefaultUserConfig()},
		{name: "flat", vic: flatent.NewDefaultUserConfig()},
		{name: "hnsw, asynchronous indexing", vic: hnswent.NewDefaultUserConfig(), async: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.async {
				t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "100ms")
			}
			crashBetweenTheVectorIndexAndTheLogOfTheObject(t, tt.vic, tt.async)
		})
	}
}

func crashBetweenTheVectorIndexAndTheLogOfTheObject(t *testing.T, vic schemaConfig.VectorIndexConfig, async bool) {
	const (
		class = "PutCrash"
		dims  = 32
		n     = 50
	)
	ctx := context.Background()
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: class}, vic, false, async)
	shard := underlyingShard(t, shd)
	r := rand.New(rand.NewSource(13))
	for _, err := range shard.PutObjectBatch(ctx, createRandomObjects(r, class, n, dims)) {
		require.NoError(t, err)
	}
	if async {
		waitForVectorQueue(t, shard, "")
	}
	// Everything so far is on disk, as after any request.
	require.NoError(t, shard.store.WriteWALs())

	// The put, up to the vector index, and the files as a crash leaves them.
	obj := createRandomObjects(r, class, 1, dims)[0]
	id, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
	require.NoError(t, err)
	status, err := shard.putObjectLSM(ctx, obj, id)
	require.NoError(t, err)
	require.NoError(t, shard.updateVectorIndex(ctx, obj.Vector, status, ""))
	files := map[string][]byte{}
	require.NoError(t, filepath.WalkDir(shard.path(), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(shard.path(), path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[rel] = data
		return nil
	}))

	// The node restarts on those files.
	dir := shard.path()
	require.NoError(t, shard.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(dir))
	for rel, data := range files {
		path := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, data, 0o644))
	}
	restoredLike, err := idx.initShard(ctx, shard.name, idx.getClass(), nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shard.name, restoredLike)
	restored := underlyingShard(t, restoredLike)
	if async {
		waitForVectorQueue(t, restored, "")
	}
	vi, release, ok := restored.AcquireVectorIndex("")
	require.True(t, ok)
	defer release()
	got, err := restored.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
	require.NoError(t, err)
	objectBack := got != nil
	vectorBack := vi.ContainsDoc(status.docID)
	t.Logf("after the crash: the object of doc %d is back: %v; its vector: %v", status.docID, objectBack, vectorBack)
	assert.Equal(t, objectBack, vectorBack,
		"doc %d: the object and its vector come back both or neither (object %v, vector %v)",
		status.docID, objectBack, vectorBack)
}
