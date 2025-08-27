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
	"fmt"
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
		RNGFactor:                 1.5,
		MaxDistanceRatio:          1.5,
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
	distancer := distancer.NewCosineDistanceProvider()
	quantizer := compressionhelpers.NewRotationalQuantizer(vectorSize, 42, 8, distancer)
	bytesInRqOutput := 80
	lsmStore, err := NewLSMStore(store, int32(bytesInRqOutput), storeBucketName)
	require.NoError(t, err)
	pages := uint64(512)
	pageSize := uint64(8192)
	versionMap := NewVersionMap(pages, pageSize)
	idGenerator := common.NewUint64Counter(0)
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
		vectorSize:   int32(bytesInRqOutput),
	}
	ctx := t.Context()
	spfresh.Start(ctx)
	vectorID := uint64(1)
	err = spfresh.Add(ctx, vectorID, []float32{0.9, 0.9})
	err = spfresh.Add(ctx, vectorID+1, []float32{0.1, 0.1})
	err = spfresh.Add(ctx, vectorID+2, []float32{0.5, 0.5})
	require.NoError(t, err)
	allowList := helpers.NewAllowList(vectorID, vectorID+1, vectorID+2)
	results, dists, err := spfresh.SearchByVector(ctx, []float32{0.9, 0.9}, 3, allowList)
	require.NoError(t, err)
	require.Equal(t, 3, len(results))
	require.Equal(t, vectorID, results[0])
	require.Equal(t, vectorID+2, results[1])
	require.Equal(t, vectorID+1, results[2])
	fmt.Println(dists) // TODO check dists
}
