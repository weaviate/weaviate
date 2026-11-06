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

// centroidIndex is one way of answering the centroid queries: a single
// posting before the bootstrap threshold, an HNSW graph after it.
type centroidIndex interface {
	Get(id uint64) (*Centroid, error)
	Insert(id uint64, centroid *Centroid) error
	Delete(id uint64) error
	Exists(id uint64) bool
	Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error)
	MaxID() uint64
	Shutdown(ctx context.Context) error
	Flush() error
	PrepareForBackup(ctx context.Context) error
	ResumeAfterBackup(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup(ctx context.Context)
}

// centroidMode lets an atomic pointer hold the current implementation.
type centroidMode struct {
	centroidIndex
}

// CentroidIndex is the centroid index HFresh queries. It starts as a single
// posting and switches to an HNSW graph on Build.
type CentroidIndex struct {
	metrics *Metrics
	store   *lsmkv.Store
	cfg     *Config
	mode    atomic.Pointer[centroidMode]
	counter atomic.Int32
	buildMu sync.Mutex
}

// NewCentroidIndex prepares the centroid index. With build set the graph is
// created right away; otherwise Build creates it once the single posting
// crosses the bootstrap threshold.
func NewCentroidIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, build bool) (*CentroidIndex, error) {
	index := CentroidIndex{
		metrics: metrics,
		store:   store,
		cfg:     cfg,
	}
	index.mode.Store(&centroidMode{&singleCentroid{}})

	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, nil
	}
	cfg.Centroids.HNSWConfig.WaitForCachePrefill = true
	cfg.Centroids.HNSWConfig.AcornFilterRatio = math.MaxFloat64

	if build {
		err := index.Build()
		if err != nil {
			return nil, err
		}
	}

	return &index, nil
}

func (c *CentroidIndex) current() centroidIndex {
	return c.mode.Load().centroidIndex
}

// Build creates the centroid graph and moves the single posting's centroid
// into it, if there is one. It is idempotent.
func (c *CentroidIndex) Build() error {
	c.buildMu.Lock()
	defer c.buildMu.Unlock()

	single, ok := c.current().(*singleCentroid)
	if !ok {
		return nil
	}

	graph, err := newHNSWCentroids(c.store, c.cfg)
	if err != nil {
		return err
	}

	id, centroid := single.snapshot()
	// the commit log may already hold it if a previous build was cut short
	if id != 0 && !graph.Exists(id) {
		err = graph.Insert(id, &Centroid{Uncompressed: centroid})
		if err != nil {
			return errors.Wrap(err, "add single posting centroid to hnsw")
		}
	}

	c.mode.Store(&centroidMode{graph})
	return nil
}

// Built reports whether the centroid graph exists.
func (c *CentroidIndex) Built() bool {
	_, ok := c.current().(*hnswCentroids)
	return ok
}

// restoreSingle reinstates the single posting recorded before a restart.
func (c *CentroidIndex) restoreSingle(id uint64, centroid []float32) {
	c.mode.Store(&centroidMode{&singleCentroid{id: id, centroid: centroid}})
	c.counter.Store(1)
}

func (c *CentroidIndex) Get(id uint64) (*Centroid, error) {
	return c.current().Get(id)
}

func (c *CentroidIndex) Insert(id uint64, centroid *Centroid) error {
	if c.Exists(id) {
		return nil
	}

	err := c.current().Insert(id, centroid)
	if err != nil {
		return err
	}
	c.counter.Add(1)

	return nil
}

func (c *CentroidIndex) MarkAsDeleted(id uint64) error {
	if !c.Exists(id) {
		return nil
	}
	c.counter.Add(-1)
	return c.current().Delete(id)
}

func (c *CentroidIndex) Exists(id uint64) bool {
	return c.current().Exists(id)
}

func (c *CentroidIndex) Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error) {
	start := time.Now()
	defer c.metrics.CentroidSearchDuration(start)

	return c.current().Search(query, k, allowList)
}

func (c *CentroidIndex) GetMaxID() uint64 {
	return c.current().MaxID()
}

func (c *CentroidIndex) Shutdown(ctx context.Context) error {
	return c.current().Shutdown(ctx)
}

func (c *CentroidIndex) Flush() error {
	return c.current().Flush()
}

func (c *CentroidIndex) PrepareForBackup(ctx context.Context) error {
	return c.current().PrepareForBackup(ctx)
}

func (c *CentroidIndex) ResumeAfterBackup(ctx context.Context) error {
	return c.current().ResumeAfterBackup(ctx)
}

func (c *CentroidIndex) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return c.current().ListFiles(ctx, basePath)
}

func (c *CentroidIndex) PostStartup(ctx context.Context) {
	c.current().PostStartup(ctx)
}

// singleCentroid serves the one posting that holds every vector before the
// bootstrap threshold. It keeps nothing on disk.
type singleCentroid struct {
	mu       sync.RWMutex
	id       uint64 // 0 until the first posting exists
	centroid []float32
}

func (s *singleCentroid) snapshot() (uint64, []float32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id, s.centroid
}

func (s *singleCentroid) Get(id uint64) (*Centroid, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.id == 0 || id != s.id {
		return nil, errors.Wrapf(ErrPostingNotFound, "centroid %d", id)
	}
	return &Centroid{Uncompressed: s.centroid}, nil
}

func (s *singleCentroid) Insert(id uint64, centroid *Centroid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id != 0 {
		return errors.Errorf("centroid index not built: cannot add posting %d next to %d", id, s.id)
	}
	s.id = id
	s.centroid = slices.Clone(centroid.Uncompressed)
	return nil
}

func (s *singleCentroid) Delete(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == s.id {
		s.id = 0
		s.centroid = nil
	}
	return nil
}

func (s *singleCentroid) Exists(id uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id != 0 && id == s.id
}

// Search ignores the allow list: the posting scan applies it per entry.
func (s *singleCentroid) Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := &ResultSet{}
	if s.id != 0 {
		results.data = append(results.data, Result{ID: s.id, Distance: 0})
	}
	return results, nil
}

func (s *singleCentroid) MaxID() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.id == 0 {
		return 0
	}
	return s.id + 1
}

func (s *singleCentroid) Shutdown(ctx context.Context) error          { return nil }
func (s *singleCentroid) Flush() error                                { return nil }
func (s *singleCentroid) PrepareForBackup(ctx context.Context) error  { return nil }
func (s *singleCentroid) ResumeAfterBackup(ctx context.Context) error { return nil }
func (s *singleCentroid) PostStartup(ctx context.Context)             {}

func (s *singleCentroid) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

// hnswCentroids serves the centroids from an HNSW graph with 8-bit RQ codes.
type hnswCentroids struct {
	hnsw *hnsw.HNSW
}

func newHNSWCentroids(store *lsmkv.Store, cfg *Config) (*hnswCentroids, error) {
	var userConfig ent.UserConfig
	userConfig.SetDefaults()
	userConfig.EF = 64
	userConfig.EFConstruction = 64
	userConfig.RQ.Enabled = true
	userConfig.RQ.Bits = 8
	userConfig.RQ.RescoreLimit = 0
	userConfig.FilterStrategy = ent.FilterStrategyAcorn

	h, err := hnsw.New(*cfg.Centroids.HNSWConfig, userConfig, cfg.TombstoneCallbacks, store)
	if err != nil {
		return nil, err
	}
	h.PostStartup(context.Background())

	return &hnswCentroids{hnsw: h}, nil
}

func (g *hnswCentroids) Get(id uint64) (*Centroid, error) {
	vec, err := g.hnsw.Get(id)
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

func (g *hnswCentroids) Insert(id uint64, centroid *Centroid) error {
	err := g.hnsw.Add(context.Background(), id, centroid.Uncompressed)
	if err != nil {
		return errors.Wrap(err, "add to hnsw")
	}
	return nil
}

func (g *hnswCentroids) Delete(id uint64) error {
	return g.hnsw.Delete(id)
}

func (g *hnswCentroids) Exists(id uint64) bool {
	return g.hnsw.ContainsDoc(id)
}

func (g *hnswCentroids) Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error) {
	ids, distances, err := g.hnsw.SearchByVector(context.TODO(), query, k, allowList)
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

func (g *hnswCentroids) MaxID() uint64 {
	return g.hnsw.CurrentVectorsLen()
}

func (g *hnswCentroids) Shutdown(ctx context.Context) error {
	return g.hnsw.Shutdown(ctx)
}

func (g *hnswCentroids) Flush() error {
	return g.hnsw.Flush()
}

func (g *hnswCentroids) PrepareForBackup(ctx context.Context) error {
	return g.hnsw.PrepareForBackup(ctx)
}

func (g *hnswCentroids) ResumeAfterBackup(ctx context.Context) error {
	return g.hnsw.ResumeAfterBackup(ctx)
}

func (g *hnswCentroids) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return g.hnsw.ListFiles(ctx, basePath)
}

func (g *hnswCentroids) PostStartup(ctx context.Context) {
	g.hnsw.PostStartup(ctx)
}
