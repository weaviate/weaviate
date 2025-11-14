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
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var _ CentroidIndex = (*HNSWIndex)(nil)

type HNSWIndex struct {
	metrics   *Metrics
	hnsw      *hnsw.HNSW
	centroids *common.PagedArray[atomic.Pointer[Centroid]]
	counter   atomic.Int32
	ids       *xsync.Map[uint64, struct{}]
}

func NewHNSWIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, pages, pageSize uint64) (*HNSWIndex, error) {
	index := HNSWIndex{
		metrics:   metrics,
		centroids: common.NewPagedArray[atomic.Pointer[Centroid]](pages, pageSize),
		ids:       xsync.NewMap[uint64, struct{}](),
	}

	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		centroid := index.Get(id)
		if centroid == nil {
			return nil, errors.New("not found")
		}
		return centroid.Uncompressed, nil
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

func (i *HNSWIndex) Get(id uint64) *Centroid {
	page, slot := i.centroids.GetPageFor(id)
	if page == nil {
		return nil
	}

	return page[slot].Load()
}

func (i *HNSWIndex) Insert(id uint64, centroid *Centroid) error {
	i.ids.Store(id, struct{}{})

	page, slot := i.centroids.EnsurePageFor(id)
	if page == nil {
		return errors.New("failed to allocate page")
	}

	page[slot].Store(centroid)

	err := i.hnsw.Add(context.Background(), id, centroid.Uncompressed)
	if err != nil {
		return errors.Wrap(err, "add to hnsw")
	}

	i.metrics.SetPostings(int(i.counter.Add(1)))

	return nil
}

func (i *HNSWIndex) MarkAsDeleted(id uint64) error {
	for {
		page, slot := i.centroids.GetPageFor(id)
		if page == nil {
			return nil
		}
		centroid := page[slot].Load()
		if centroid == nil {
			return errors.New("centroid not found")
		}

		if centroid.Deleted {
			return errors.New("centroid already marked as deleted")
		}

		newCentroid := Centroid{
			Uncompressed: centroid.Uncompressed,
			Compressed:   centroid.Compressed,
			Deleted:      true,
		}

		if page[slot].CompareAndSwap(centroid, &newCentroid) {
			i.metrics.SetPostings(int(i.counter.Add(-1)))
			break
		}
	}

	i.ids.Delete(id)

	return i.hnsw.Delete(id)
}

func (i *HNSWIndex) Exists(id uint64) bool {
	centroid := i.Get(id)
	if centroid == nil {
		return false
	}

	return !centroid.Deleted
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
