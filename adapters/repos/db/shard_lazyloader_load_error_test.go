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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// A NewShard failure has to reach the caller as a chain: the reindex cleanup
// sweep's truncation guard reads the cause to tell a run that ran out of time
// from a shard that broke, and a flattened error answers that with "no".
// Reproduces https://github.com/weaviate/weaviate/issues/12663.
func TestLazyLoadShardLoadPreservesTheNewShardErrorChain(t *testing.T) {
	const (
		className = "Movies"
		shardName = "shard-with-an-unopenable-store"
	)

	ctx := context.Background()
	_, idx := testShard(t, ctx, className)

	// os.MkdirAll reports a *fs.PathError for an existing non-directory on every
	// platform, so the cause this test follows is the same everywhere.
	shardDir := filepath.Join(idx.path(), shardName)
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "lsm"), []byte("not a directory"), 0o644))

	lazy := &LazyLoadShard{
		shardOpts:        &deferredShardOpts{name: shardName, index: idx, class: &models.Class{Class: className}},
		memMonitor:       &loadAttemptMonitor{admitLoad: true},
		shardLoadLimiter: newSweepLoadLimiter(),
	}

	loadErr := lazy.Load(ctx)
	require.Error(t, loadErr)

	var pathErr *fs.PathError
	require.ErrorAs(t, loadErr, &pathErr,
		"Load must wrap what NewShard reported, not reformat it into a new error")
	require.NotNil(t, errors.Unwrap(loadErr))
	require.ErrorContains(t, loadErr, shardName)
}
