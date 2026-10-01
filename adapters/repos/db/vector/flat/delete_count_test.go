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

package flat

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// Regression for #12958: Delete must keep AlreadyIndexed() aligned with the
// live vector set (ContainsDoc), not leave a stale post-delete count.
func TestFlatDeleteUpdatesAlreadyIndexedCount(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	store, err := lsmkv.New(root, root, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(context.Background())

	cfg := flatent.UserConfig{}
	cfg.SetDefaults()

	idx, err := New(Config{
		ID:                "delete-count",
		RootPath:          root,
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeRegularBucketOptions,
	}, cfg, store)
	require.NoError(t, err)
	defer idx.Shutdown(context.Background())

	require.NoError(t, idx.Add(context.Background(), 7, []float32{1, 0}))
	require.NoError(t, idx.Add(context.Background(), 8, []float32{0, 1}))

	before := idx.AlreadyIndexed()
	require.Equal(t, uint64(2), before)

	require.NoError(t, idx.Delete(7))

	after := idx.AlreadyIndexed()
	require.Equal(t, uint64(1), after)
	require.False(t, idx.ContainsDoc(7))
	require.True(t, idx.ContainsDoc(8))

	// Deleting a missing id must not underflow the live count.
	require.NoError(t, idx.Delete(7))
	require.Equal(t, uint64(1), idx.AlreadyIndexed())
}
