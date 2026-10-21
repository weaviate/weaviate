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

package lsmkv

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestStoreListFilesMigrationDir pins what an active shard's backup takes from
// .migrations: every tracker sentinel, but none of the scratch files an
// interrupted atomic write leaves behind.
func TestStoreListFilesMigrationDir(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dir := t.TempDir()
	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	trackerDir := filepath.Join(dir, ".migrations", "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(trackerDir, 0o755))
	for _, name := range []string{"started.mig", "properties.mig", "progress.mig.000000001"} {
		require.NoError(t, os.WriteFile(filepath.Join(trackerDir, name), []byte("x"), 0o644))
	}

	// Same call the tracker's atomic properties.mig write makes, so the name
	// carries the real random infix rather than one the test picked.
	leftover, err := os.CreateTemp(trackerDir, "properties.mig.*.tmp")
	require.NoError(t, err)
	require.NoError(t, leftover.Close())

	got, err := store.ListFiles(ctx, dir)
	require.NoError(t, err)
	sort.Strings(got)

	require.Equal(t, []string{
		filepath.Join(".migrations", "searchable_retokenize_text_1", "progress.mig.000000001"),
		filepath.Join(".migrations", "searchable_retokenize_text_1", "properties.mig"),
		filepath.Join(".migrations", "searchable_retokenize_text_1", "started.mig"),
	}, got)
}
