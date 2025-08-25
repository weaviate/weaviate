//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func Test_SPFresh_Add_Search(t *testing.T) {
	userConfig := UserConfig{
		MaxPostingSize:            100,
		MinPostingSize:            10,
		SplitWorkers:              5,
		ReassignWorkers:           5,
		InternalPostingCandidates: 4,
		ReassignNeighbors:         4,
		Replicas:                  1,
		RNGFactor:                 0.5,
		MaxDistanceRatio:          0.5,
	}
	logger, _ := test.NewNullLogger()
	promMetrics := monitoring.GetMetrics()
	className := "class"
	shardName := "shard"
	metrics := lsmkv.NewMetrics(promMetrics, className, shardName)
	shardCompactionCallbacks := cyclemanager.NewCallbackGroup("shard-compaction", logger, 1)
	shardFlushCallbacks := cyclemanager.NewCallbackGroup("shard-flush", logger, 1)
	store, err := lsmkv.New(t.TempDir(), t.TempDir(), logger, metrics, shardCompactionCallbacks, shardCompactionCallbacks, shardFlushCallbacks)
	require.NoError(t, err)
	vectorSize := 2
	storeBucketName := "bucket"
	lsmStore, err := NewLSMStore(store, int32(vectorSize), storeBucketName)
	require.NoError(t, err)
	pages := uint64(512)
	pageSize := uint64(8192)
	versionMap := NewVersionMap(pages, pageSize)
	idGenerator := common.NewUint64Counter(0)
	distancer := distancer.NewCosineDistanceProvider()
	quantizer := compressionhelpers.NewRotationalQuantizer(vectorSize, 42, 8, distancer)
	sptag := NewBruteForceSPTAG(quantizer)
	spfresh := SPFresh{
		Logger:       logrus.NewEntry(logger),
		UserConfig:   &userConfig,
		SPTAG:        sptag,
		Store:        lsmStore,
		VersionMap:   versionMap,
		IDs:          idGenerator,
		PostingSizes: NewPostingSizes(pages, pageSize),
		Quantizer:    quantizer,
		Distancer:    distancer,
	}
	ctx := t.Context()
	spfresh.Start(ctx)
	vectorID := uint64(1)
	err = spfresh.Add(ctx, vectorID, []float32{0.9, 0.9})
	require.NoError(t, err)
	allowList := helpers.NewAllowList(vectorID)
	results, dists, err := spfresh.SearchByVector(ctx, []float32{0.9, 0.9}, 1, allowList)
	require.NoError(t, err)
	require.Equal(t, 1, len(results)) // TODO fails with 0
	require.Equal(t, vectorID, results[0])
	require.Equal(t, 1, len(dists))
	require.InDelta(t, float32(0), dists[0], 0.000001)
}
