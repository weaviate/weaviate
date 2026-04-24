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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type Centroid struct {
	Uncompressed []float32
	Compressed   []byte
	Deleted      bool
	MedoidID     uint64 // ID of the real vector that represents this cluster
}

func (c *Centroid) Distance(distancer *Distancer, v Vector) (float32, error) {
	return v.DistanceWithRaw(distancer, c.Compressed)
}

// MedoidVectorProvider retrieves the original vector for a posting's medoid.
// This is used by HNSW for rescore operations.
type MedoidVectorProvider struct {
	medoidStore *MedoidStore
	vectorForID func(ctx context.Context, id uint64) ([]float32, error)
}

func (m *MedoidVectorProvider) GetVector(ctx context.Context, postingID uint64) ([]float32, error) {
	if m.medoidStore == nil || m.vectorForID == nil {
		return nil, nil
	}
	medoidID, found, err := m.medoidStore.Get(ctx, postingID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return m.vectorForID(ctx, medoidID)
}

type HNSWIndex struct {
	metrics        *Metrics
	hnsw           *hnsw.HNSW
	counter        atomic.Int32
	quantizer      *compressionhelpers.BinaryRotationalQuantizer
	vectorProvider *MedoidVectorProvider
}

// defaultCentroidRescoreLimit is the multiplier for k to determine the rescore limit.
// With RQ8, 1.5x gives good recall/performance tradeoff.
const defaultCentroidRescoreMultiplier = 1.5

func NewHNSWIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, pages, pageSize uint64) (*HNSWIndex, error) {
	vectorProvider := &MedoidVectorProvider{}

	index := HNSWIndex{
		metrics:        metrics,
		vectorProvider: vectorProvider,
	}

	// VectorForIDThunk is used by HNSW for non-compressed distance calculations
	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return vectorProvider.GetVector(ctx, id)
	}

	// TempVectorForIDWithViewThunk is used by HNSW rescore operations
	// For HFresh centroids, the view is ignored since we fetch medoid vectors from a different store
	cfg.Centroids.HNSWConfig.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		return vectorProvider.GetVector(ctx, id)
	}

	var userConfig ent.UserConfig
	userConfig.SetDefaults()
	userConfig.EF = 64
	userConfig.EFConstruction = 64
	userConfig.RQ.Enabled = true
	userConfig.RQ.Bits = 8
	// RescoreLimit = 1.5 * EF to allow accurate distance computation with medoid vectors
	userConfig.RQ.RescoreLimit = int(math.Ceil(defaultCentroidRescoreMultiplier * float64(userConfig.EF)))
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

// SetVectorProvider configures the medoid vector provider for rescore operations.
// This must be called after HFresh initialization to enable accurate distance computation.
func (i *HNSWIndex) SetVectorProvider(medoidStore *MedoidStore, vectorForID func(ctx context.Context, id uint64) ([]float32, error)) {
	i.vectorProvider.medoidStore = medoidStore
	i.vectorProvider.vectorForID = vectorForID
}

func (i *HNSWIndex) SetQuantizer(quantizer *compressionhelpers.BinaryRotationalQuantizer) {
	i.quantizer = quantizer
}

func (i *HNSWIndex) Get(id uint64) (*Centroid, error) {
	vec, err := i.hnsw.Get(id)
	if err != nil {
		return nil, err
	}
	cmp, err := hnsw.GetCompressedVector[byte](i.hnsw, id)
	if err != nil {
		return nil, err
	}

	return &Centroid{
		Uncompressed: vec,
		Compressed:   cmp,
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
