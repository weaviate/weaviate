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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestLazyLoadShardDropBlocksReload pins that a shard which was never loaded is
// not instantiated after it was dropped, which would recreate the directory the
// drop just removed.
func TestLazyLoadShardDropBlocksReload(t *testing.T) {
	const shardName = "lazyshard"

	_, idx := testShard(t, t.Context(), "LazyDrop")
	class := &models.Class{Class: idx.Config.ClassName.String()}

	shardLike, err := idx.initShard(t.Context(), shardName, class, nil, false, true)
	require.NoError(t, err)
	lazy, ok := shardLike.(*LazyLoadShard)
	require.True(t, ok)

	require.NoError(t, os.MkdirAll(shardPath(idx.path(), shardName), 0o755))
	require.NoError(t, lazy.drop(false))

	require.ErrorIs(t, lazy.Load(t.Context()), errAlreadyShutdown)
	_, err = lazy.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)
	require.NoDirExists(t, shardPath(idx.path(), shardName))
}
