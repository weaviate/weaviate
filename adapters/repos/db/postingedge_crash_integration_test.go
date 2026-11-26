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
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/postingedge"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	postingedgeent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// A put stores the object in the store, then gives its vector to the vector
// index, and only then has the store write its buffered logs (putOne:
// putObjectLSM, updateVectorIndex, WriteWALs). The process dies between the
// last two: what is on disk is the files as they are at that point, without
// what the store still buffers. The object and its vector come back after the
// restart both or neither.
//
// Two places the vector can be when the process dies:
//   - the index flushed, as updateVectorIndex has it do when the index is not
//     asynchronous: the index empties its journal, so the store writes the
//     log of the objects first (PersistentConfig.WriteObjectsLog), and both
//     come back;
//   - the index took the vector and did not flush yet: it is in the journal
//     only, which the index applies again when it opens, and the object is
//     not in the store: the index asks the shard and drops it
//     (PersistentConfig.ObjectExists).
func TestPostingEdge_CrashBetweenTheIndexAndTheLogOfTheObject(t *testing.T) {
	for _, tt := range []struct {
		name string
		// flushed has the vector reach the index as a put does, flush
		// included, and otherwise through Add alone.
		flushed bool
		// back is whether the object and its vector come back.
		back bool
	}{
		{name: "the index flushed", flushed: true, back: true},
		{name: "the vector is in the journal only", flushed: false, back: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			crashBetweenTheIndexAndTheLogOfTheObject(t, tt.flushed, tt.back)
		})
	}
}

func crashBetweenTheIndexAndTheLogOfTheObject(t *testing.T, flushed, back bool) {
	const (
		class = "PostingEdgeCrash"
		dims  = 32
		n     = 400
	)
	ctx := context.Background()
	uc := postingedgeent.NewDefaultUserConfig()
	uc.MaxPostingSizeKB = postingedgeent.MinimumMaxPostingSizeKB
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: class}, uc, false, false,
		func(i *Index) { i.Config.PostingEdgeBaseSize = 150 })
	shard := underlyingShard(t, shd)
	r := rand.New(rand.NewSource(13))
	for _, err := range shard.PutObjectBatch(ctx, createRandomObjects(r, class, n, dims)) {
		require.NoError(t, err)
	}
	dirs, err := filepath.Glob(filepath.Join(shard.path(), "*.postingedge.d"))
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dirs[0], "meta.json"))
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the index builds its base")
	inner, release, ok := shard.AcquireVectorIndex("")
	require.True(t, ok)
	release()
	persistent, ok := inner.(*postingedge.Persistent)
	require.True(t, ok, "the vector index of the class is a %T", inner)
	require.NoError(t, persistent.Settle(ctx))
	// Everything so far is on disk, as after any request.
	require.NoError(t, shard.store.WriteWALs())
	require.NoError(t, persistent.Flush())

	// The put, up to the vector index, and the files as a crash leaves them.
	obj := createRandomObjects(r, class, 1, dims)[0]
	id, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
	require.NoError(t, err)
	status, err := shard.putObjectLSM(ctx, obj, id)
	require.NoError(t, err)
	if flushed {
		require.NoError(t, shard.updateVectorIndex(ctx, obj.Vector, status, ""))
	} else {
		require.NoError(t, persistent.Add(ctx, status.docID, obj.Vector))
		journal, err := os.Stat(filepath.Join(dirs[0], "journal.log"))
		require.NoError(t, err)
		require.Positive(t, journal.Size(), "the vector is in the journal")
	}
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

	restored := restorePostingEdgeShardFrom(t, idx, shard, files)
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
	assert.Equal(t, back, objectBack, "doc %d: the object is back", status.docID)
	next := createRandomObjects(r, class, 1, dims)[0]
	assert.NoError(t, restored.PutObject(ctx, next), "an object put after the restart is indexed")
}
