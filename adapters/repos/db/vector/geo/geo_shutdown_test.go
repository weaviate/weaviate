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

package geo

import (
	"context"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"syscall"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// descriptorsOnDir counts this process's descriptors on the regular files in dir, matched by device and inode.
func descriptorsOnDir(t *testing.T, dir string) int {
	t.Helper()

	type fileID struct {
		dev uint64
		ino uint64
	}
	files := map[fileID]struct{}{}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := os.Stat(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		st := info.Sys().(*syscall.Stat_t)
		files[fileID{dev: uint64(st.Dev), ino: st.Ino}] = struct{}{}
	}

	fds, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)
	count := 0
	for _, entry := range fds {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		var st syscall.Stat_t
		// ReadDir closed the descriptor it listed /dev/fd through, so Fstat fails on it
		if err := syscall.Fstat(fd, &st); err != nil {
			continue
		}
		if _, ok := files[fileID{dev: uint64(st.Dev), ino: st.Ino}]; ok {
			count++
		}
	}
	return count
}

func newPersistedGeoIndex(t *testing.T, rootPath string, elements []models.GeoCoordinates) *Index {
	t.Helper()

	nullLogger, _ := logrustest.NewNullLogger()
	index, err := NewIndex(Config{
		AllocChecker: memwatch.NewDummyMonitor(),
		ID:           "geo-shutdown",
		CoordinatesForID: func(ctx context.Context, id uint64) (*models.GeoCoordinates, error) {
			return &elements[id], nil
		},
		RootPath: rootPath,
		Logger:   nullLogger,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	index.PostStartup(context.Background())
	return index
}

func TestGeoIndexShutdownReleasesCommitLog(t *testing.T) {
	elements := []models.GeoCoordinates{
		{Latitude: ptFloat32(48.13743), Longitude: ptFloat32(11.57549)},
		{Latitude: ptFloat32(48.78232), Longitude: ptFloat32(9.17702)},
		{Latitude: ptFloat32(52.52000), Longitude: ptFloat32(13.40500)},
	}

	tests := []struct {
		name      string
		flushed   int
		unflushed int
	}{
		{name: "nothing added"},
		{name: "all adds flushed", flushed: 3},
		{name: "adds after the last flush", flushed: 1, unflushed: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previous := debug.SetGCPercent(-1)
			t.Cleanup(func() { debug.SetGCPercent(previous) })
			ctx := context.Background()
			root := t.TempDir()
			dir := filepath.Join(root, "geo-shutdown.hnsw.commitlog.d")

			index := newPersistedGeoIndex(t, root, elements)
			for i := range tt.flushed {
				require.NoError(t, index.Add(ctx, uint64(i), &elements[i]))
			}
			require.NoError(t, index.Flush())
			for i := tt.flushed; i < tt.flushed+tt.unflushed; i++ {
				require.NoError(t, index.Add(ctx, uint64(i), &elements[i]))
			}
			require.Equal(t, 1, descriptorsOnDir(t, dir))

			require.NoError(t, index.Shutdown(ctx))
			require.Zero(t, descriptorsOnDir(t, dir), "geo index shutdown left its commit log open")

			reloaded := newPersistedGeoIndex(t, root, elements)
			ids, err := reloaded.WithinRange(ctx, filters.GeoRange{
				GeoCoordinates: &models.GeoCoordinates{Latitude: ptFloat32(50), Longitude: ptFloat32(11)},
				Distance:       2_000_000,
			})
			require.NoError(t, err)
			require.Len(t, ids, tt.flushed+tt.unflushed, "geo entries are missing after reload")
			require.NoError(t, reloaded.Shutdown(ctx))
		})
	}
}
