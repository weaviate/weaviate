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
	"slices"
	"sync"
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

// HNSWIndex is the centroid index. Below the bootstrap threshold every
// vector lives in one posting and no graph exists: hnsw stays nil and the
// single posting's id and centroid are served from singleID/singleCentroid.
// Build creates the graph and moves that posting into it.
type HNSWIndex struct {
	metrics *Metrics
	store   *lsmkv.Store
	cfg     *Config
	hnsw    atomic.Pointer[hnsw.HNSW]
	counter atomic.Int32

	buildMu sync.Mutex

	singleMu       sync.RWMutex
	singleID       uint64
	singleCentroid []float32
}

// NewHNSWIndex prepares the centroid index. With build set the graph is
// created right away; otherwise it is created by Build once the first
// posting crosses the bootstrap threshold.
func NewHNSWIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, build bool) (*HNSWIndex, error) {
	index := HNSWIndex{
		metrics: metrics,
		store:   store,
		cfg:     cfg,
	}

	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, nil
	}
	cfg.Centroids.HNSWConfig.WaitForCachePrefill = true
	cfg.Centroids.HNSWConfig.AcornFilterRatio = math.MaxFloat64

	if build {
		if err := index.Build(); err != nil {
			return nil, err
		}
	}

	return &index, nil
}

// Build creates the centroid graph and inserts the single posting's
// centroid into it, if there is one. It is idempotent.
func (i *HNSWIndex) Build() error {
	i.buildMu.Lock()
	defer i.buildMu.Unlock()

	if i.hnsw.Load() != nil {
		return nil
	}

	var userConfig ent.UserConfig
	userConfig.SetDefaults()
	userConfig.EF = 64
	userConfig.EFConstruction = 64
	userConfig.RQ.Enabled = true
	userConfig.RQ.Bits = 8
	userConfig.RQ.RescoreLimit = 0
	userConfig.FilterStrategy = ent.FilterStrategyAcorn

	h, err := hnsw.New(*i.cfg.Centroids.HNSWConfig, userConfig, i.cfg.TombstoneCallbacks, i.store)
	if err != nil {
		return err
	}
	h.PostStartup(context.Background())

	i.singleMu.RLock()
	defer i.singleMu.RUnlock()
	// the commit log may already hold it if a previous build was cut short
	if i.singleID != 0 && !h.ContainsDoc(i.singleID) {
		err = h.Add(context.Background(), i.singleID, i.singleCentroid)
		if err != nil {
			return errors.Wrap(err, "add single posting centroid to hnsw")
		}
	}

	i.hnsw.Store(h)
	return nil
}

// restoreSingle reinstates the single posting recorded before a restart.
func (i *HNSWIndex) restoreSingle(id uint64, centroid []float32) {
	i.singleMu.Lock()
	defer i.singleMu.Unlock()
	i.singleID = id
	i.singleCentroid = centroid
	i.counter.Store(1)
}

// Built reports whether the centroid graph exists.
func (i *HNSWIndex) Built() bool {
	return i.hnsw.Load() != nil
}

func (i *HNSWIndex) Shutdown(ctx context.Context) error {
	if h := i.hnsw.Load(); h != nil {
		return h.Shutdown(ctx)
	}
	return nil
}

func (i *HNSWIndex) Flush() error {
	if h := i.hnsw.Load(); h != nil {
		return h.Flush()
	}
	return nil
}

func (i *HNSWIndex) PrepareForBackup(ctx context.Context) error {
	if h := i.hnsw.Load(); h != nil {
		return h.PrepareForBackup(ctx)
	}
	return nil
}

func (i *HNSWIndex) ResumeAfterBackup(ctx context.Context) error {
	if h := i.hnsw.Load(); h != nil {
		return h.ResumeAfterBackup(ctx)
	}
	return nil
}

func (i *HNSWIndex) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	if h := i.hnsw.Load(); h != nil {
		return h.ListFiles(ctx, basePath)
	}
	return nil, nil
}

func (i *HNSWIndex) PostStartup(ctx context.Context) {
	if h := i.hnsw.Load(); h != nil {
		h.PostStartup(ctx)
	}
}

func (i *HNSWIndex) Get(id uint64) (*Centroid, error) {
	h := i.hnsw.Load()
	if h == nil {
		i.singleMu.RLock()
		defer i.singleMu.RUnlock()
		if i.singleID == 0 || id != i.singleID {
			return nil, errors.Wrapf(ErrPostingNotFound, "centroid %d", id)
		}
		return &Centroid{Uncompressed: i.singleCentroid}, nil
	}

	vec, err := h.Get(id)
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

	h := i.hnsw.Load()
	if h == nil {
		i.singleMu.Lock()
		defer i.singleMu.Unlock()
		if i.singleID != 0 {
			return errors.Errorf("centroid index not built: cannot add posting %d next to %d", id, i.singleID)
		}
		i.singleID = id
		i.singleCentroid = slices.Clone(centroid.Uncompressed)
		i.counter.Add(1)
		return nil
	}

	err := h.Add(context.Background(), id, centroid.Uncompressed)
	if err != nil {
		return errors.Wrap(err, "add to hnsw")
	}
	i.counter.Add(1)

	return nil
}

func (i *HNSWIndex) MarkAsDeleted(id uint64) error {
	if !i.Exists(id) {
		return nil
	}
	i.counter.Add(-1)

	h := i.hnsw.Load()
	if h == nil {
		i.singleMu.Lock()
		defer i.singleMu.Unlock()
		i.singleID = 0
		i.singleCentroid = nil
		return nil
	}
	return h.Delete(id)
}

func (i *HNSWIndex) Exists(id uint64) bool {
	h := i.hnsw.Load()
	if h == nil {
		i.singleMu.RLock()
		defer i.singleMu.RUnlock()
		return i.singleID != 0 && id == i.singleID
	}
	return h.ContainsDoc(id)
}

func (i *HNSWIndex) Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error) {
	start := time.Now()
	defer i.metrics.CentroidSearchDuration(start)

	h := i.hnsw.Load()
	if h == nil {
		// one posting holds everything: the allow list is applied by the
		// posting scan instead
		i.singleMu.RLock()
		defer i.singleMu.RUnlock()
		results := &ResultSet{}
		if i.singleID != 0 {
			results.data = append(results.data, Result{ID: i.singleID, Distance: 0})
		}
		return results, nil
	}

	ids, distances, err := h.SearchByVector(context.TODO(), query, k, allowList)
	if err != nil {
		return nil, err
	}

	results := make([]Result, len(ids))
	for i := range ids {
		results[i] = Result{ID: ids[i], Distance: distances[i]}
	}

	// The HNSW heap breaks distance ties in arbitrary order (the centroid
	// index's 8-bit RQ distances tie for ~0.1% of candidates on the 10M
	// benchmark), which would make posting selection — and therefore
	// filtered results — vary run to run. Order ties by ascending id so
	// the ranked list is deterministic.
	slices.SortStableFunc(results, func(a, b Result) int {
		switch {
		case a.Distance < b.Distance:
			return -1
		case a.Distance > b.Distance:
			return 1
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})

	return &ResultSet{data: results}, nil
}

func (i *HNSWIndex) GetMaxID() uint64 {
	h := i.hnsw.Load()
	if h == nil {
		i.singleMu.RLock()
		defer i.singleMu.RUnlock()
		if i.singleID == 0 {
			return 0
		}
		return i.singleID + 1
	}
	return h.CurrentVectorsLen()
}
