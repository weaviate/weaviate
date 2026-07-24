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

package datasets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCachedParquetPath(t *testing.T) {
	const (
		datasetID = "weaviate/ann-datasets"
		subset    = "fiqa-st-minilm-384-dot-12k"
		sha       = "1e437271d862b9ed45ebc169a1bcd0b39c16def7"
	)
	relFile := subset + "/train/train.parquet"

	// seedCache lays out a HuggingFace-style cache under a temp XDG_CACHE_HOME and
	// returns the repo directory. The caller decides which pieces to populate.
	seedCache := func(t *testing.T) string {
		t.Helper()
		root := t.TempDir()
		t.Setenv("XDG_CACHE_HOME", root)
		return filepath.Join(root, "huggingface", "hub", "datasets--weaviate--ann-datasets")
	}
	writeInfo := func(t *testing.T, repoDir, body string) {
		t.Helper()
		infoPath := filepath.Join(repoDir, "info", "main")
		require.NoError(t, os.MkdirAll(filepath.Dir(infoPath), 0o755))
		require.NoError(t, os.WriteFile(infoPath, []byte(body), 0o644))
	}
	writeSnapshot := func(t *testing.T, repoDir string) string {
		t.Helper()
		p := filepath.Join(repoDir, "snapshots", sha, filepath.FromSlash(relFile))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte("parquet"), 0o644))
		return p
	}

	t.Run("hit when info and snapshot present", func(t *testing.T) {
		repoDir := seedCache(t)
		writeInfo(t, repoDir, `{"sha":"`+sha+`"}`)
		want := writeSnapshot(t, repoDir)

		h := NewHubDataset(datasetID, subset)
		got, ok := h.cachedParquetPath(relFile)
		require.True(t, ok)
		require.Equal(t, want, got)
	})

	t.Run("miss when info file absent", func(t *testing.T) {
		repoDir := seedCache(t)
		writeSnapshot(t, repoDir)

		h := NewHubDataset(datasetID, subset)
		_, ok := h.cachedParquetPath(relFile)
		require.False(t, ok)
	})

	t.Run("miss when snapshot absent", func(t *testing.T) {
		repoDir := seedCache(t)
		writeInfo(t, repoDir, `{"sha":"`+sha+`"}`)

		h := NewHubDataset(datasetID, subset)
		_, ok := h.cachedParquetPath(relFile)
		require.False(t, ok)
	})

	t.Run("miss when sha empty", func(t *testing.T) {
		repoDir := seedCache(t)
		writeInfo(t, repoDir, `{"sha":""}`)
		writeSnapshot(t, repoDir)

		h := NewHubDataset(datasetID, subset)
		_, ok := h.cachedParquetPath(relFile)
		require.False(t, ok)
	})
}
