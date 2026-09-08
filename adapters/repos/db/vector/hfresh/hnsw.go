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

package hfresh

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type Centroid struct {
	Uncompressed []float32
	Compressed   []byte
	Deleted      bool
}

func (c *Centroid) Distance(distancer *Distancer, v Vector) (float32, error) {
	// Centroids fetched via Centroids.Get carry no code (the centroid HNSW
	// stores 8-bit RQ codes, which this 1-bit distancer cannot read), so
	// encode lazily on first use and memoize. The unsynchronized write below
	// relies on Centroid values being confined to a single goroutine within
	// one maintenance operation (Get returns a fresh instance per call).
	// Sharing a Centroid across goroutines — e.g. caching instances to skip
	// the Get — would make this a data race with a torn slice-header read;
	// add synchronization here before introducing any such sharing.
	if c.Compressed == nil {
		if distancer == nil || distancer.quantizer == nil {
			return 0, errors.New("centroid distancer is not initialized")
		}
		c.Compressed = distancer.quantizer.CompressedBytes(distancer.quantizer.Encode(c.Uncompressed))
	}
	dist, err := v.DistanceWithRaw(distancer, c.Compressed)
	if err != nil {
		return 0, err
	}
	// The split/merge reassignment gates compare these distances with plain
	// <, >= — NaN makes every comparison false and silently disables the
	// gates instead of failing the operation, so reject it here.
	if math.IsNaN(float64(dist)) {
		return 0, errors.Errorf("NaN distance between vector %d and centroid (incompatible code formats?)", v.ID())
	}
	return dist, nil
}

type HNSWIndex struct {
	metrics *Metrics
	hnsw    *hnsw.HNSW
	counter atomic.Int32
}

func NewHNSWIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, pages, pageSize uint64) (*HNSWIndex, error) {
	index := HNSWIndex{
		metrics: metrics,
	}

	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, nil
	}

	var userConfig ent.UserConfig
	userConfig.SetDefaults()
	userConfig.EF = 64
	userConfig.EFConstruction = 64
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

func (i *HNSWIndex) Get(id uint64) (*Centroid, error) {
	vec, err := i.hnsw.Get(id)
	if err != nil {
		return nil, err
	}

	// Compressed is left nil on purpose: the centroid HNSW stores 8-bit RQ
	// codes, which Centroid.Distance cannot use. It encodes a 1-bit code
	// lazily on first use, so hot callers that only read Uncompressed
	// (RNGSelect on the insert path) don't pay for an encode.
	return &Centroid{
		Uncompressed: vec,
		Deleted:      false,
	}, nil
}

func (i *HNSWIndex) Insert(id uint64, centroid *Centroid) error {
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

func (i *HNSWIndex) MarkAsDeleted(id uint64) error {
	if i.Exists(id) {
		i.counter.Add(-1)
		return i.hnsw.Delete(id)
	}
	return nil
}

func (i *HNSWIndex) Exists(id uint64) bool {
	return i.hnsw.ContainsDoc(id)
}

func (i *HNSWIndex) Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error) {
	start := time.Now()
	defer i.metrics.CentroidSearchDuration(start)

	ids, distances, err := i.hnsw.SearchByVector(context.TODO(), query, k, allowList)
	if err != nil {
		return nil, err
	}

	results := make([]Result, len(ids))
	for i := range ids {
		results[i] = Result{ID: ids[i], Distance: distances[i]}
	}

	return &ResultSet{data: results}, nil
}

func (i *HNSWIndex) GetMaxID() uint64 {
	return i.hnsw.CurrentVectorsLen()
}
