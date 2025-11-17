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
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var _ CentroidIndex = (*HNSWIndex)(nil)

type HNSWIndex struct {
	metrics   *Metrics
	hnsw      *hnsw.HNSW
	counter   atomic.Int32
	quantizer *compressionhelpers.RotationalQuantizer
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

	h, err := hnsw.New(*cfg.Centroids.HNSWConfig, userConfig, cfg.TombstoneCallbacks, store)
	if err != nil {
		return nil, err
	}

	index.hnsw = h

	return &index, nil
}

func (i *HNSWIndex) SetQuantizer(quantizer *compressionhelpers.RotationalQuantizer) {
	i.quantizer = quantizer
}

func (i *HNSWIndex) Get(id uint64) *Centroid {
	vec, err := i.hnsw.Get(id)
	if err != nil {
		return nil
	}
	return &Centroid{
		Uncompressed: vec,
		Compressed:   i.quantizer.Encode(vec),
		Deleted:      false,
	}
}

func (i *HNSWIndex) Insert(id uint64, centroid *Centroid) error {
	if i.Exists(id) {
		return nil
	}

	err := i.hnsw.Add(context.Background(), id, centroid.Uncompressed)
	if err != nil {
		return errors.Wrap(err, "add to hnsw")
	}
	i.metrics.SetPostings(int(i.counter.Add(1)))

	return nil
}

func (i *HNSWIndex) MarkAsDeleted(id uint64) error {
	if i.Exists(id) {
		i.metrics.SetPostings(int(i.counter.Add(-1)))
		return i.hnsw.Delete(id)
	}
	return nil
}

func (i *HNSWIndex) Exists(id uint64) bool {
	return i.hnsw.ContainsDoc(id)
}

func (i *HNSWIndex) Search(query []float32, k int) (*ResultSet, error) {
	start := time.Now()
	defer i.metrics.CentroidSearchDuration(start)

	ids, distances, err := i.hnsw.SearchByVector(context.TODO(), query, k, nil)
	if err != nil {
		return nil, err
	}

	results := make([]Result, len(ids))
	for i := range ids {
		results[i] = Result{ID: ids[i], Distance: distances[i]}
	}

	return &ResultSet{data: results}, nil
}
