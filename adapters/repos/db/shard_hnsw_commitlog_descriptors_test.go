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
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fileIdentity struct {
	dev uint64
	ino uint64
}

// collectCommitLogFiles adds the identity of every file under an hnsw commit log directory below root to seen, and returns how many such directories exist.
func collectCommitLogFiles(t *testing.T, root string, seen map[fileIdentity]struct{}) int {
	t.Helper()

	dirs := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if strings.HasSuffix(d.Name(), ".hnsw.commitlog.d") {
				dirs++
			}
			return nil
		}
		if !strings.HasSuffix(filepath.Dir(path), ".hnsw.commitlog.d") {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		seen[fileIdentity{dev: uint64(st.Dev), ino: st.Ino}] = struct{}{}
		return nil
	})
	require.NoError(t, err)
	return dirs
}

// descriptorsOnFiles counts this process's descriptors whose device and inode are in files, deleted files included.
func descriptorsOnFiles(t *testing.T, files map[fileIdentity]struct{}) int {
	t.Helper()

	entries, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)

	count := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		var st syscall.Stat_t
		// ReadDir closed the descriptor it listed /dev/fd through, so Fstat fails on it
		if err := syscall.Fstat(fd, &st); err != nil {
			continue
		}
		if _, ok := files[fileIdentity{dev: uint64(st.Dev), ino: st.Ino}]; ok {
			count++
		}
	}
	return count
}

func TestShardShutdownReleasesHnswCommitLogs(t *testing.T) {
	previous := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(previous) })

	ctx := context.Background()
	const (
		className = "HnswCommitLogDescriptors"
		cycles    = 5
		objects   = 5
	)
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "location",
			DataType: schema.DataTypeGeoCoordinates.PropString(),
		}},
	}
	shard, index := testShardWithSettings(t, ctx, class, nil, false, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			"first":  enthnsw.NewDefaultUserConfig(),
			"second": enthnsw.NewDefaultUserConfig(),
		}
	})
	s, ok := shard.(*Shard)
	require.True(t, ok)

	for i := range objects {
		lat, lon := float32(48+i), float32(11+i)
		require.NoError(t, s.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]interface{}{"location": &models.GeoCoordinates{Latitude: &lat, Longitude: &lon}},
			},
			Vectors: map[string][]float32{
				"first":  {float32(i), 1, 2},
				"second": {1, float32(i), 2},
			},
		}))
	}

	shardPath := s.path()
	seen := map[fileIdentity]struct{}{}
	for cycle := range cycles {
		current := map[fileIdentity]struct{}{}
		require.Equal(t, 2, collectCommitLogFiles(t, shardPath, current),
			"cycle %d: expected one commit log per named vector and none for the geo property, which is an lsmkv bucket", cycle)
		require.Equal(t, 2, descriptorsOnFiles(t, current),
			"cycle %d: a loaded shard keeps one commit log open per hnsw index", cycle)
		for file := range current {
			seen[file] = struct{}{}
		}

		require.NoError(t, s.Shutdown(ctx))
		require.Zero(t, descriptorsOnFiles(t, seen), "cycle %d: shutdown left hnsw commit logs open", cycle)

		var err error
		s, err = NewShard(ctx, nil, s.Name(), index, class, index.centralJobQueue, index.scheduler,
			index.shardReindexer, false, index.bitmapBufPool, monitoring.ShardRegistrationEager)
		require.NoError(t, err)

		for _, target := range []string{"first", "second"} {
			vectorIndex, release, ok := s.AcquireVectorIndex(target)
			require.True(t, ok)
			for docID := range uint64(objects) {
				require.True(t, vectorIndex.ContainsDoc(docID), "cycle %d: %s lost doc %d", cycle, target, docID)
			}
			release()
		}
	}

	collectCommitLogFiles(t, shardPath, seen)
	require.NoError(t, s.Shutdown(ctx))
	require.Zero(t, descriptorsOnFiles(t, seen), "final shutdown left hnsw commit logs open")
}
