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
	"io/fs"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/storobj"
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

// singlePostingID is the posting that holds every vector before the centroid
// graph exists.
const singlePostingID = 1

// HNSWIndex holds the centroids in an HNSW graph. A lazy index has no graph
// until its first split: posting 1 holds every vector and has no centroid.
type HNSWIndex struct {
	metrics *Metrics
	store   *lsmkv.Store
	cfg     *Config
	counter atomic.Int32

	state atomic.Pointer[centroidState]
	// pending receives the children of the first split, and becomes the
	// graph when the split deletes posting 1
	pending *hnsw.HNSW
	buildMu sync.Mutex
}

// centroidState is replaced as a whole, so a reader never combines the graph
// of one moment with the single posting of another.
type centroidState struct {
	// nil in single-posting mode
	hnsw *hnsw.HNSW
	// single reports whether posting 1 exists, while there is no graph
	single bool
}

// load returns the graph and, without one, whether posting 1 exists.
func (i *HNSWIndex) load() (*hnsw.HNSW, bool) {
	state := i.state.Load()
	return state.hnsw, state.single
}

// NewHNSWIndex creates the centroid graph, reloading it from its commit log.
// A lazy index only does so if the graph was created before, which left its
// commit log directory.
func NewHNSWIndex(metrics *Metrics, store *lsmkv.Store, cfg *Config, lazy bool) (*HNSWIndex, error) {
	index := HNSWIndex{
		metrics: metrics,
		store:   store,
		cfg:     cfg,
	}
	index.state.Store(&centroidState{})

	cfg.Centroids.HNSWConfig.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, nil
	}
	cfg.Centroids.HNSWConfig.WaitForCachePrefill = true
	cfg.Centroids.HNSWConfig.AcornFilterRatio = math.MaxFloat64

	if lazy {
		_, err := os.Stat(hnsw.CommitLogDirectory(cfg.Centroids.HNSWConfig.RootPath, cfg.Centroids.HNSWConfig.ID))
		if errors.Is(err, fs.ErrNotExist) {
			return &index, nil
		}
		if err != nil {
			return nil, errors.Wrap(err, "check the centroid graph's commit log")
		}
	}

	h, err := index.newGraph()
	if err != nil {
		return nil, err
	}
	index.state.Store(&centroidState{hnsw: h})

	return &index, nil
}

func (i *HNSWIndex) newGraph() (*hnsw.HNSW, error) {
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
		return nil, err
	}
	h.PostStartup(context.Background())

	return h, nil
}

// Built reports whether the centroid graph exists.
func (i *HNSWIndex) Built() bool {
	g, _ := i.load()
	return g != nil
}

// restoreSingle reinstates the single posting found on disk at a restart.
func (i *HNSWIndex) restoreSingle() {
	i.state.Store(&centroidState{single: true})
	i.counter.Store(1)
}

func (i *HNSWIndex) Get(id uint64) (*Centroid, error) {
	g, single := i.load()
	if g == nil {
		if id != singlePostingID || !single {
			return nil, errors.Wrapf(ErrPostingNotFound, "centroid %d", id)
		}
		// no vector: with a single candidate nothing compares it
		return &Centroid{}, nil
	}

	vec, err := g.Get(id)
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

	g, single := i.load()
	if g == nil {
		if id == singlePostingID {
			i.state.Store(&centroidState{single: true})
			i.counter.Add(1)
			return nil
		}
		if !single {
			return errors.Errorf("centroid graph not built: cannot add posting %d", id)
		}

		// a child of the first split
		var err error
		i.buildMu.Lock()
		if i.pending == nil {
			i.pending, err = i.newGraph()
		}
		g = i.pending
		i.buildMu.Unlock()
		if err != nil {
			return errors.Wrap(err, "create the centroid graph")
		}
	}

	err := g.Add(context.Background(), id, centroid.Uncompressed)
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

	g, _ := i.load()
	if g == nil {
		// the first split deletes posting 1 once its children are in the
		// pending graph: one store replaces the posting by the graph
		i.buildMu.Lock()
		defer i.buildMu.Unlock()
		i.state.Store(&centroidState{hnsw: i.pending})
		i.pending = nil
		return nil
	}
	return g.Delete(id)
}

func (i *HNSWIndex) Exists(id uint64) bool {
	g, single := i.load()
	if g == nil {
		return id == singlePostingID && single
	}
	return g.ContainsDoc(id)
}

// VectorMissing reports whether a centroid's stored vector is gone, as a
// crash that saves the commit log but not the compressed vectors leaves it.
// The single posting has no stored vector to lose.
func (i *HNSWIndex) VectorMissing(id uint64) bool {
	g, _ := i.load()
	if g == nil {
		return false
	}

	_, err := hnsw.GetCompressedVector[byte](g, id)
	var notFound storobj.ErrNotFound
	return errors.As(err, &notFound)
}

func (i *HNSWIndex) Search(query []float32, k int, allowList helpers.AllowList) (*ResultSet, error) {
	start := time.Now()
	defer i.metrics.CentroidSearchDuration(start)

	g, single := i.load()
	if g == nil {
		// the allow list is left to the posting scan, which applies it per entry
		var results ResultSet
		if single {
			results.data = append(results.data, Result{ID: singlePostingID})
		}
		return &results, nil
	}

	// the per-request search params never reach the centroid graph; the
	// request's searchProbe is applied to the posting budget instead
	ids, distances, err := g.SearchByVector(context.TODO(), query, k, allowList, nil)
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
	g, single := i.load()
	if g == nil {
		if single {
			return singlePostingID + 1
		}
		return 0
	}
	return g.CurrentVectorsLen()
}

func (i *HNSWIndex) Shutdown(ctx context.Context) error {
	g, _ := i.load()
	if g == nil {
		// a first split that failed halfway leaves its graph pending
		i.buildMu.Lock()
		g = i.pending
		i.buildMu.Unlock()
	}
	if g == nil {
		return nil
	}
	return g.Shutdown(ctx)
}

func (i *HNSWIndex) Flush() error {
	g, _ := i.load()
	if g == nil {
		return nil
	}
	return g.Flush()
}

func (i *HNSWIndex) PrepareForBackup(ctx context.Context) error {
	g, _ := i.load()
	if g == nil {
		return nil
	}
	return g.PrepareForBackup(ctx)
}

func (i *HNSWIndex) ResumeAfterBackup(ctx context.Context) error {
	g, _ := i.load()
	if g == nil {
		return nil
	}
	return g.ResumeAfterBackup(ctx)
}

func (i *HNSWIndex) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	g, _ := i.load()
	if g == nil {
		return nil, nil
	}
	return g.ListFiles(ctx, basePath)
}

func (i *HNSWIndex) PostStartup(ctx context.Context) {
	g, _ := i.load()
	if g != nil {
		g.PostStartup(ctx)
	}
}
