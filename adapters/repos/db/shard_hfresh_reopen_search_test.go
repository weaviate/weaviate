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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

// Found in passing while checking the backups of HFresh. An append updates
// HFresh's posting map in memory only (PostingMap.FastAddVectorID); the
// analyze task it enqueues persists it later. A reopened shard restores the
// persisted map (HFresh.restoreMetadata) and replays the analyze queue on the
// scheduler, and until the replay runs every vector search over a posting
// whose analysis had not run before the shutdown returns nothing, with no
// error. With the scheduler ticking once a second, that is the first second
// after the load at least; a lazy shard loads on its first query.
func TestHFreshSearchesRightAfterReopen(t *testing.T) {
	ctx := testCtx()
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "HFreshReopen"},
		enthfresh.NewDefaultUserConfig(), false, false)
	shard := underlyingShard(t, shd)

	objs := createRandomObjects(rand.New(rand.NewSource(1)), "HFreshReopen", 20, 32)
	putAll(t, shard, objs)
	requireAllSearchable(t, shard, objs, len(objs))

	require.NoError(t, shard.Shutdown(ctx))
	sl, err := idx.initShard(ctx, shard.name, idx.getClass(), nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shard.name, sl)
	reopened := underlyingShard(t, sl)

	found := 0
	for _, o := range objs {
		if vectorStateOf(t, reopened, o.ID(), o.Vector, len(objs)).inSearch {
			found++
		}
	}
	assert.Equalf(t, len(objs), found,
		"right after the reopen, %d of %d objects are found by vector search, with no error", found, len(objs))

	// once the analyze queue has replayed, everything is back
	settleHFresh(t, reopened)
	requireAllSearchable(t, reopened, objs, len(objs))
}
