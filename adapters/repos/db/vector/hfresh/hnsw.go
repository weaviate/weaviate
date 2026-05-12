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
	version        uint8 // Index version for version-aware behavior
}

// DefaultPostingRescoreLimit is the HNSW posting-level representative rescore limit for V2+ indexes.
// This is distinct from the final vector rescore limit (DefaultHFreshRescoreLimit = 350).
//
// The posting rescore limit controls how many HNSW posting representatives (medoids) are rescored
// with full-precision vectors during the centroid search phase. Since each posting can contain
// ~100 vectors (for 1536-dimensional data), rescoring too many postings is expensive.
//
// The final vector rescore limit (rq.rescoreLimit in user config) controls how many actual
// vectors are rescored at the final stage after all posting candidates have been collected.
//
// V1 indexes use 0 (no posting rescoring) because they don't have medoid mappings.
// V2+ indexes use 10 by default to balance recall and performance.
const DefaultPostingRescoreLimit = 100

// rqBitsForVersion returns the RQ bit depth based on index version.
// V1: RQ8 (8-bit) - traditional compression, no medoid rescoring
// V2: RQ1 (1-bit) - aggressive compression with medoid rescoring for accuracy
func rqBitsForVersion(version uint8) int16 {
	if version >= HFreshIndexVersion2 {
		return 1 // RQ1 for V2+
	}
	return 8 // RQ8 for V1
}

// rqRescoreLimitForVersion returns the HNSW posting-level RQ rescore limit based on index version.
// V1: 0 (no posting rescoring - V1 has no medoid mappings)
// V2+: DefaultPostingRescoreLimit (10) (rescoring enabled with medoid vectors)
//
// Note: This is separate from the final vector rescore limit (DefaultHFreshRescoreLimit = 350)
// which controls how many vectors are rescored at the final stage after posting search.
func rqRescoreLimitForVersion(version uint8) int {
	if version >= HFreshIndexVersion2 {
		return DefaultPostingRescoreLimit
	}
	return 0 // V1: no HNSW posting rescoring
}

func NewHNSWIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, version uint8, pages, pageSize uint64) (*HNSWIndex, error) {
	vectorProvider := &MedoidVectorProvider{}

	index := HNSWIndex{
		metrics:        metrics,
		vectorProvider: vectorProvider,
		version:        version,
	}

	// Configure vector provider thunks based on version.
	// V2+: Wire to medoid vector provider for HNSW RQ rescoring.
	// V1: Return error if called - V1 should never attempt medoid rescoring
	//     because RQ.RescoreLimit is 0.
	if version >= HFreshIndexVersion2 {
		cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
			return vectorProvider.GetVector(ctx, id)
		}
		cfg.Centroids.HNSWConfig.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			return vectorProvider.GetVector(ctx, id)
		}
	} else {
		// V1: These thunks should never be called since RQ.RescoreLimit is 0.
		// Return an error to catch any unexpected calls during testing.
		cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, errors.New("unexpected medoid rescore for HFresh V1 index")
		}
		cfg.Centroids.HNSWConfig.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			return nil, errors.New("unexpected medoid rescore for HFresh V1 index")
		}
	}

	var userConfig ent.UserConfig
	userConfig.SetDefaults()
	userConfig.EF = 64
	userConfig.EFConstruction = 64
	userConfig.RQ.Enabled = true
	userConfig.RQ.Bits = rqBitsForVersion(version)
	userConfig.RQ.RescoreLimit = rqRescoreLimitForVersion(version)
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

// RQBits returns the RQ bit depth used by this HNSW index.
func (i *HNSWIndex) RQBits() int16 {
	return rqBitsForVersion(i.version)
}

// RQRescoreLimit returns the HNSW posting-level RQ rescore limit used by this index.
// V1: 0 (no posting rescoring), V2+: DefaultPostingRescoreLimit (10)
// This is the posting/HNSW representative rescore limit, not the final vector rescore limit.
func (i *HNSWIndex) RQRescoreLimit() int {
	return rqRescoreLimitForVersion(i.version)
}

func (i *HNSWIndex) Get(id uint64) (*Centroid, error) {
	vec, err := i.hnsw.Get(id)
	if err != nil {
		return nil, err
	}

	// Compress using HFresh's quantizer format (not HNSW's internal RQ format)
	// This is needed because Centroid.Distance uses HFresh's distancer which
	// expects HFresh's compression format
	var compressed []byte
	if i.quantizer != nil {
		compressed = i.quantizer.CompressedBytes(i.quantizer.Encode(vec))
	}

	return &Centroid{
		Uncompressed: vec,
		Compressed:   compressed,
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
