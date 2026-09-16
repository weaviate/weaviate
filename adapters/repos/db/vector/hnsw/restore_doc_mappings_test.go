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

package hnsw

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type restoreDocNoopBucketView struct{}

func (n *restoreDocNoopBucketView) ReleaseView() {}

// newRestoreDocMappingsIndex builds the multivector index shared by the
// TestRestoreDocMappings* tests.
func newRestoreDocMappingsIndex(t *testing.T) *hnsw {
	t.Helper()

	uc := ent.UserConfig{}
	uc.Multivector.Enabled = true

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "doc-mappings",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, nil
		},
		GetViewThunk: func() common.BucketView { return &restoreDocNoopBucketView{} },
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			return nil, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	return index
}

func TestRestoreDocMappingsWithMissingBucket(t *testing.T) {
	index := newRestoreDocMappingsIndex(t)

	err := index.AddMulti(context.Background(), 1, [][]float32{{1, 2, 3}})
	assert.Nil(t, err)

	newStore := testinghelpers.NewDummyStore(t)
	index.store = newStore
	assert.Nil(t, err)
	err = index.restoreDocMappings()
	assert.ErrorContains(t, err, "multivector mappings bucket not found")
}

func TestRestoreDocMappingsWithNilData(t *testing.T) {
	index := newRestoreDocMappingsIndex(t)

	err := index.AddMulti(context.Background(), 1, [][]float32{{1, 2, 3}})
	assert.Nil(t, err)
	err = index.AddMulti(context.Background(), 2, [][]float32{{4, 5, 6}, {7, 8, 9}})
	assert.Nil(t, err)
	nodeIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nodeIDBytes, 2)
	err = index.store.Bucket(index.id + "_mv_mappings").Delete(nodeIDBytes)
	assert.Nil(t, err)
	err = index.store.Bucket(index.id+"_mv_mappings").Put(nodeIDBytes, []byte{5})
	require.Nil(t, err)
	err = index.restoreDocMappings()
	require.Nil(t, err)
	assert.Nil(t, index.nodes[2])

	// a node removed by the startup repair must be tombstoned (in memory —
	// the commit logger is not wired up yet at this point of startup) so
	// that the next cleanup cycle reassigns the edges still pointing at it
	// and, if it was the entrypoint, moves the entrypoint off the dangling
	// id. Without the tombstone nothing ever revisits the removed node.
	index.tombstoneLock.Lock()
	_, tombstoned := index.tombstones[2]
	index.tombstoneLock.Unlock()
	assert.True(t, tombstoned, "node removed by startup repair must carry a tombstone")
}

// TestRestoreDocMappingsTombstonesRemovedEntrypoint: when the node removed by
// the startup repair is the entrypoint, the tombstone is what makes the next
// cleanup cycle move the entrypoint off the dangling id (the cycle's handling
// of a tombstoned nil-slot entrypoint is pinned in delete_test.go). Without
// it the entrypoint dangles forever: nodes[ep] is nil but no tombstone means
// no cleanup path ever looks at it.
func TestRestoreDocMappingsTombstonesRemovedEntrypoint(t *testing.T) {
	index := newRestoreDocMappingsIndex(t)

	err := index.AddMulti(context.Background(), 1, [][]float32{{1, 2, 3}})
	require.Nil(t, err)
	err = index.AddMulti(context.Background(), 2, [][]float32{{4, 5, 6}})
	require.Nil(t, err)

	ep := index.getEntrypoint()
	nodeIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nodeIDBytes, ep)
	err = index.store.Bucket(index.id + "_mv_mappings").Delete(nodeIDBytes)
	require.Nil(t, err)

	err = index.restoreDocMappings()
	require.Nil(t, err)

	require.Nil(t, index.nodes[ep], "entrypoint node must be removed")
	index.tombstoneLock.Lock()
	_, tombstoned := index.tombstones[ep]
	index.tombstoneLock.Unlock()
	assert.True(t, tombstoned,
		"removed entrypoint must carry a tombstone so cleanup can move the entrypoint")
}
