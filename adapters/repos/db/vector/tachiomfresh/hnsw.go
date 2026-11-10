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

package tachiomfresh

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// HNSWIndex is the centroid index. copied from hfresh.HNSWIndex, which
// hard-codes efConstruction and ef to 64 and keeps the hnsw field
// unexported. The copy exposes efConstruction, so that one can check that
// it does not cap recall. It adds Distancer and Norm2, which the placement
// of tokens reads, and its Search takes the caller's context. The rest is
// as in hfresh.
//
// The graph stores an RQ-8 code per centroid (NewHNSWIndex), and two
// distances are computed on those codes. A query token searches the graph
// with the index distance, the dot product (Search). A document token is
// placed in the posting of the centroid of its type that is nearest in
// squared L2 distance (nearestCentroids in types.go). The split clusters by L2 (hfresh's
// 2-means, kmeans_encoder.go), so a token belongs with the centroid
// nearest to it in L2. The dot product favours the centroid of largest
// norm, regardless of its direction. The L2 distance is computed on the
// same codes, with nothing decoded:
//
//	|v - c|^2 = |v|^2 + |c|^2 - 2 <v, c>
//
// <v, c> is the dot estimate of the code (Distancer) and |c|^2 the squared
// norm the code stores (Norm2). The split, merge and reassign checks
// compare squared L2 distances as well (centroidDistance in split.go). Each
// check asks whether a token is closer to one centroid than to another, and
// the answer must agree with the placement.
type HNSWIndex struct {
	metrics *hfresh.Metrics
	hnsw    *hnsw.HNSW
	counter atomic.Int32
}

// copied from hfresh.NewHNSWIndex, with efConstruction as a parameter. ef
// stays at hfresh's 64; it can be changed through hnsw.UpdateUserConfig.
func NewHNSWIndex(metrics *hfresh.Metrics, store *lsmkv.Store, cfg *hfresh.Config, pages, pageSize uint64, efConstruction int) (*HNSWIndex, error) {
	index := HNSWIndex{
		metrics: metrics,
	}

	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, nil
	}

	var userConfig ent.UserConfig
	userConfig.SetDefaults()
	userConfig.EF = 64
	userConfig.EFConstruction = efConstruction
	userConfig.RQ.Enabled = true
	userConfig.RQ.Bits = 8
	userConfig.RQ.RescoreLimit = 0
	userConfig.FilterStrategy = ent.FilterStrategyAcorn
	cfg.Centroids.HNSWConfig.WaitForCachePrefill = true
	cfg.Centroids.HNSWConfig.AcornFilterRatio = math.MaxFloat64

	h, err := hnsw.New(*cfg.Centroids.HNSWConfig, userConfig, cfg.TombstoneCallbacks, store)
	if err != nil {
		return nil, err
	}
	h.PostStartup(context.Background())

	index.hnsw = h

	return &index, nil
}

// copied from hfresh.HNSWIndex.Get
func (i *HNSWIndex) Get(id uint64) (*hfresh.Centroid, error) {
	vec, err := i.hnsw.Get(id)
	if err != nil {
		return nil, err
	}

	return &hfresh.Centroid{
		Uncompressed: vec,
		Deleted:      false,
	}, nil
}

// copied from hfresh.HNSWIndex.Insert
func (i *HNSWIndex) Insert(id uint64, centroid *hfresh.Centroid) error {
	if i.Exists(id) {
		return nil
	}

	err := i.hnsw.Add(context.Background(), id, centroid.Uncompressed)
	if err != nil {
		return errors.Wrap(err, "add to hnsw")
	}
	i.counter.Add(1)

	return nil
}

// copied from hfresh.HNSWIndex.MarkAsDeleted
func (i *HNSWIndex) MarkAsDeleted(id uint64) error {
	if i.Exists(id) {
		i.counter.Add(-1)
		return i.hnsw.Delete(id)
	}
	return nil
}

// copied from hfresh.HNSWIndex.Exists
func (i *HNSWIndex) Exists(id uint64) bool {
	return i.hnsw.ContainsDoc(id)
}

// copied from hfresh.HNSWIndex.Search, with the caller's context in place
// of context.TODO. hfresh fills ResultSet.data directly. That field is
// unexported, so the results are inserted one by one. Insert keeps them
// ordered by distance, ties by id ascending.
func (i *HNSWIndex) Search(ctx context.Context, query []float32, k int, allowList helpers.AllowList) (*hfresh.ResultSet, error) {
	start := time.Now()
	defer i.metrics.CentroidSearchDuration(start)

	ids, distances, err := i.hnsw.SearchByVector(ctx, query, k, allowList)
	if err != nil {
		return nil, err
	}

	results := hfresh.NewResultSet(len(ids))
	for i := range ids {
		results.Insert(ids[i], distances[i])
	}

	return results, nil
}

// Distancer returns the distance from query to any centroid by id, computed
// on the RQ-8 codes the centroid index stores, the same distances Search
// compares. Not in hfresh: the scan over the centroids of one token type
// uses it (nearestCentroids). Close it when done.
func (i *HNSWIndex) Distancer(query []float32) common.QueryVectorDistancer {
	return i.hnsw.QueryVectorDistancer(query)
}

// Norm2 returns the squared norm of centroid id as its RQ-8 code stores it
// (RQCode.Norm2: the exact float32 norm of the vector the centroid was
// inserted as; the rotation preserves it). Not in hfresh: the L2 placement
// of tokens reads it next to the dot estimate of the code
// (nearestCentroids; see the HNSWIndex doc). The code comes from the
// compressor's cache, the same read that Distancer's distances make.
func (i *HNSWIndex) Norm2(id uint64) (float32, error) {
	code, err := hnsw.GetCompressedVector[byte](i.hnsw, id)
	if err != nil {
		return 0, err
	}
	return compressionhelpers.RQCode(code).Norm2(), nil
}
