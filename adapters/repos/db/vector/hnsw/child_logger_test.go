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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestChildLoggersCarryTheIndexID pins that the logger hnsw hands to the
// components it builds (here: the compressor) already carries hnsw's OWN
// index_id, overriding whatever id the caller's logger had. The shard hands
// hfresh's centroid graph a logger identified as the parent index; a
// compressor line still labeled with the parent id would misattribute
// centroid maintenance. The compressor's recovery path is the one child line
// that can be provoked deterministically: a compressed entry that is missing
// while the bucket is read-only.
func TestChildLoggersCarryTheIndexID(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	const parentID, ownID = "vectors_title", "vectors_title_centroids"
	vectors := [][]float32{{1, 0, 0, 0}}
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.BQ = ent.BQConfig{Enabled: true}

	store := testinghelpers.NewDummyStore(t)
	index, err := New(Config{
		RootPath:         t.TempDir(),
		ID:               ownID,
		Logger:           logger.WithField("index_id", parentID), // what the shard hands a centroid graph
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		AllocChecker:     memwatch.NewDummyMonitor(),
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return MakeNoopCommitLogger()
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vectors[id], nil
		},
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(context.Background())

	require.NoError(t, index.Add(context.Background(), 0, vectors[0]))

	// provoke the compressor's recovery log line: drop the compressed vector
	// from the compressor's cache and bucket, then make the bucket read-only
	// so the recovery's write-back is skipped and logged
	index.compressor.Delete(context.Background(), 0)
	bucket := store.Bucket(helpers.GetCompressedBucketName("title_centroids"))
	require.NotNil(t, bucket)
	bucket.UpdateStatus(storagestate.StatusReadOnly)
	defer bucket.UpdateStatus(storagestate.StatusReady)

	_, err = index.compressor.NewDistancerFromID(0)
	require.NoError(t, err)

	var seen bool
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] != "recover_compressed_vector" {
			continue
		}
		seen = true
		require.Equalf(t, ownID, entry.Data["index_id"], "line %q", entry.Message)
	}
	require.True(t, seen, "the compressor's recovery line was not logged")
}
