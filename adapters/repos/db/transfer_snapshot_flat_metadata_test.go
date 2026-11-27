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
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	esync "github.com/weaviate/weaviate/entities/sync"
	entdynamic "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/integrity"
)

// The flat index keeps its dimensions and its RQ rotation in a bbolt file it
// rewrites in place (flat/metadata.go: setDimensions, persistRQData). A backup
// freezes it with SnapshotMutableFiles, a read transaction's copy. The replica
// snapshot hardlinks every file ListBackupFiles lists
// (collectShardRelativeFiles) and never calls SnapshotMutableFiles, so the
// staged meta file is the live inode. A write after the snapshot changes what
// the target downloads, and a write between the target's metadata request
// (size and CRC) and its download fails the checksum the copier validates.
//
// The write here is the first vector the index receives: initializeDimensionsAndRQ
// persists the dimensions, and the rotation when RQ is on.
func TestTransferSnapshotFreezesFlatMetadata(t *testing.T) {
	flatRQ8 := flatUserConfig()
	flatRQ8.RQ.Enabled = true
	flatRQ8.RQ.Bits = 8

	dynamicFlat := entdynamic.UserConfig{}
	dynamicFlat.SetDefaults()
	dynamicFlat.Threshold = 1_000_000 // stays flat
	dynamicFlat.FlatUC = flatUserConfig()
	dynamicFlat.HnswUC = enthnsw.NewDefaultUserConfig()

	indexes := []struct {
		name  string
		cfg   schemaConfig.VectorIndexConfig
		async bool // dynamic needs the async queue
	}{
		{name: "flat", cfg: flatUserConfig()},
		{name: "flat with rq8", cfg: flatRQ8},
		{name: "dynamic still flat", cfg: dynamicFlat, async: true},
	}

	journeys := []struct {
		name string
		// stage takes the journey's snapshot and returns the flat meta file's
		// path in it and a reader of the staged copy, which the transfer
		// streams later on.
		stage func(t *testing.T, idx *Index, shard *Shard) (metaRel string, read func() []byte, crcAtSnapshot uint32)
	}{
		{name: "backup", stage: stageFlatMetaByBackup},
		{name: "replica movement", stage: stageFlatMetaByReplicaSnapshot},
	}

	for _, journey := range journeys {
		t.Run(journey.name, func(t *testing.T) {
			for _, ix := range indexes {
				t.Run(ix.name, func(t *testing.T) {
					ctx := testCtx()
					shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "FlatMeta"}, ix.cfg, false, ix.async)
					shard := underlyingShard(t, shd)

					metaRel, read, crcAtSnapshot := journey.stage(t, idx, shard)
					atSnapshot := read()
					require.NotEmpty(t, atSnapshot)
					livePath := filepath.Join(shard.path(), metaRel)
					liveBefore, err := os.ReadFile(livePath)
					require.NoError(t, err)

					// the first vector after the snapshot: the live meta file is rewritten
					first := createRandomObjects(rand.New(rand.NewSource(1)), "FlatMeta", 1, 32)
					putAll(t, shard, first)
					waitQueueDrained(t, shard, "")
					liveAfter, err := os.ReadFile(livePath)
					require.NoError(t, err)
					require.NotEqual(t, liveBefore, liveAfter,
						"the first vector rewrote the live meta file, so the comparison below proves something")

					streamed := read()
					assert.Truef(t, bytes.Equal(atSnapshot, streamed),
						"the staged %s changed after the snapshot (%d bytes then, %d now): the transfer streams the live file",
						metaRel, len(atSnapshot), len(streamed))

					// what the copier checks: the CRC its metadata request returned
					// against the CRC of what it downloaded
					tmp := filepath.Join(t.TempDir(), "downloaded")
					require.NoError(t, os.WriteFile(tmp, streamed, 0o644))
					_, crcDownloaded, err := integrity.CRC32(tmp)
					require.NoError(t, err)
					assert.Equal(t, crcAtSnapshot, crcDownloaded,
						"the copier fails the movement: checksum validation of %s", metaRel)
				})
			}
		})
	}
}

// stageFlatMetaByBackup stages a backup of the live shard. A backup uploads
// the staged copy.
func stageFlatMetaByBackup(t *testing.T, idx *Index, shard *Shard) (string, func() []byte, uint32) {
	t.Helper()
	ctx := context.Background()

	staging := t.TempDir()
	var sd backup.ShardDescriptor
	files, err := shard.CreateBackupSnapshot(ctx, &sd, staging, newReindexGate(idx.db, reindexGateMaxAge))
	require.NoError(t, err)

	rootRel := findFlatMetaFile(t, files)
	staged := filepath.Join(staging, rootRel)
	read := func() []byte {
		data, err := os.ReadFile(staged)
		require.NoError(t, err)
		return data
	}
	_, crc, err := integrity.CRC32(staged)
	require.NoError(t, err)
	metaRel, err := shard.shardRelativePath(rootRel)
	require.NoError(t, err)
	return metaRel, read, crc
}

// stageFlatMetaByReplicaSnapshot creates the replica snapshot the copier of
// a movement fetches from, and asks for the meta file's metadata the way the
// copier does before it downloads.
func stageFlatMetaByReplicaSnapshot(t *testing.T, idx *Index, shard *Shard) (string, func() []byte, uint32) {
	t.Helper()
	ctx := context.Background()

	if idx.replicaSnapshotOpLocks == nil {
		idx.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
	}
	const opID = "move"
	files, err := idx.IncomingCreateReplicaSnapshot(ctx, shard.name, opID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.IncomingReleaseReplicaSnapshot(context.Background(), opID) })

	metaRel := findFlatMetaFile(t, files)
	read := func() []byte {
		rc, err := idx.IncomingGetReplicaSnapshotFile(ctx, opID, metaRel)
		require.NoError(t, err)
		defer rc.Close()
		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		return data
	}
	md, err := idx.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, metaRel)
	require.NoError(t, err)
	return metaRel, read, md.CRC32
}

func findFlatMetaFile(t *testing.T, files []string) string {
	t.Helper()
	var found []string
	for _, rel := range files {
		if entflat.IsMetadataFile(rel) {
			found = append(found, rel)
		}
	}
	require.Len(t, found, 1, "the snapshot lists the flat meta file once: %v", files)
	return found[0]
}
