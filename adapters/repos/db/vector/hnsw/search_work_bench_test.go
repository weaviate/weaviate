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

//go:build postingedgebench

// The HNSW half of the work accounting of the posting-edge benchmark (the
// timed benchmark is ../postingedge/bench_test.go): how many distances an
// HNSW search computes at each ef, and what the graph is made of, level by
// level. It lives in this package because both need the internals of the
// index. It measures no time: the counting wraps the compressor, which a
// timed run must not carry.
//
//	go test -tags postingedgebench -run TestSearchWork -timeout 0 -v \
//	    ./adapters/repos/db/vector/hnsw/ \
//	    -work.dataset ~/datasets/dbpedia-openai-1000k-angular.bin -work.out hnsw-work.json
//
// The index is built exactly like the HNSW arm of the timed benchmark:
// cosine, centered 1-bit RQ, upgraded once the training limit is reached.

package hnsw

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var (
	workDataset  = flag.String("work.dataset", "", "dataset directory (meta.txt, train.f32, test.f32, neighbors.i32)")
	workOut      = flag.String("work.out", "", "path of the JSON result file")
	workLimit    = flag.Int("work.limit", 0, "use only the first n train vectors (0 = all); ground truth is then brute-forced")
	workQueries  = flag.Int("work.queries", 2000, "number of test queries")
	workK        = flag.Int("work.k", 10, "top k")
	workEFs      = flag.String("work.efs", "10,16,24,32,48,64,96,128,192,256,384,512,768,1024", "ef sweep")
	workRescores = flag.String("work.rescores", strconv.Itoa(ent.DefaultBRQRescoreLimit), "rescore limit sweep")
	workTraining = flag.Int("work.training", ent.DefaultRQTrainingLimit, "quantizer training limit")
	workMaxConn  = flag.Int("work.maxconn", ent.DefaultMaxConnections, "max connections")
	workEFC      = flag.Int("work.efc", ent.DefaultEFConstruction, "ef construction")
)

// workCompressor counts the distances computed through the distancers it
// hands out: against stored codes, and against full-precision vectors.
type workCompressor struct {
	compressionhelpers.VectorCompressor
	toCodes   atomic.Int64
	toVectors atomic.Int64
}

func (c *workCompressor) NewDistancer(vector []float32,
) (compressionhelpers.CompressorDistancer, compressionhelpers.ReturnDistancerFn) {
	inner, release := c.VectorCompressor.NewDistancer(vector)
	return &workDistancer{inner: inner, counts: c}, release
}

type workDistancer struct {
	inner  compressionhelpers.CompressorDistancer
	counts *workCompressor
}

func (d *workDistancer) DistanceToNode(id uint64) (float32, error) {
	d.counts.toCodes.Add(1)
	return d.inner.DistanceToNode(id)
}

func (d *workDistancer) DistancesToNodes(ids []uint64, dists []float32) []error {
	d.counts.toCodes.Add(int64(len(ids)))
	return d.inner.DistancesToNodes(ids, dists)
}

func (d *workDistancer) DistanceToFloat(vec []float32) (float32, error) {
	d.counts.toVectors.Add(1)
	return d.inner.DistanceToFloat(vec)
}

type workLevel struct {
	Level int `json:"level"`
	// Nodes is the number of nodes present on the level.
	Nodes int `json:"nodes"`
	// Connections is the number of links stored on the level.
	Connections int `json:"connections"`
}

type workPoint struct {
	EF           int     `json:"ef"`
	RescoreLimit int     `json:"rescoreLimit"`
	Recall       float64 `json:"recall"`
	// CodeDistances and VectorDistances are per query: distances computed
	// against stored codes while walking the graph, and against
	// full-precision vectors while rescoring.
	CodeDistances   float64 `json:"codeDistances"`
	VectorDistances float64 `json:"vectorDistances"`
}

type workResult struct {
	Dataset string         `json:"dataset"`
	Vectors int            `json:"vectors"`
	Dims    int            `json:"dims"`
	Queries int            `json:"queries"`
	K       int            `json:"k"`
	Config  map[string]any `json:"config"`
	Levels  []workLevel    `json:"levels"`
	// PackedConnectionBytes is the serialized size of all connection lists,
	// which tracks what they hold in memory. The vertices, their locks and
	// the allocator's rounding come on top.
	PackedConnectionBytes int64       `json:"packedConnectionBytes"`
	Points                []workPoint `json:"points"`
}

func TestSearchWork(t *testing.T) {
	if *workDataset == "" {
		t.Skip("no -work.dataset given")
	}
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	provider := distancer.NewCosineDistanceProvider()

	raw, err := testinghelpers.ReadRawDataset(*workDataset, *workLimit, *workQueries)
	require.NoError(t, err)
	workParallel(len(raw.Train), func(i int) { distancer.NormalizeInPlace(raw.Train[i]) })
	workParallel(len(raw.Queries), func(i int) { distancer.NormalizeInPlace(raw.Queries[i]) })

	truth := raw.Neighbors
	if truth == nil {
		t.Logf("brute-forcing the ground truth over %d vectors", len(raw.Train))
		truth = make([][]uint64, len(raw.Queries))
		workParallel(len(raw.Queries), func(i int) {
			truth[i], _ = testinghelpers.BruteForce(logger, raw.Train, raw.Queries[i], *workK,
				testinghelpers.DistanceWrapper(provider))
		})
	}

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = *workMaxConn
	uc.EFConstruction = *workEFC
	uc.EF = 64
	uc.RQ = ent.RQConfig{
		Enabled:       true,
		Bits:          1,
		Centering:     true,
		TrainingLimit: *workTraining,
		RescoreLimit:  workInts(t, *workRescores)[0],
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "work",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      provider,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			if id >= uint64(len(raw.Train)) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return raw.Train[id], nil
		},
		TempVectorForIDWithViewThunk: func(_ context.Context, id uint64, container *common.VectorSlice,
			_ common.BucketView,
		) ([]float32, error) {
			if id >= uint64(len(raw.Train)) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			container.Slice = append(container.Slice[:0], raw.Train[id]...)
			return container.Slice, nil
		},
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)
	index.PostStartup(ctx)

	insert := func(from, to int) {
		var failed atomic.Pointer[error]
		var next atomic.Int64
		next.Store(int64(from))
		var wg sync.WaitGroup
		for range runtime.GOMAXPROCS(0) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= to {
						return
					}
					if err := index.Add(ctx, uint64(i), raw.Train[i]); err != nil {
						failed.Store(&err)
						return
					}
				}
			}()
		}
		wg.Wait()
		if err := failed.Load(); err != nil {
			require.NoError(t, *err)
		}
	}
	warm := min(uc.RQ.TrainingLimit, len(raw.Train))
	insert(0, warm)
	upgraded := make(chan struct{})
	require.NoError(t, index.Upgrade(func() { close(upgraded) }))
	<-upgraded
	require.True(t, index.Compressed(), "the index must be compressed after the upgrade")
	insert(warm, len(raw.Train))

	result := &workResult{
		Dataset: raw.Name,
		Vectors: len(raw.Train),
		Dims:    raw.Dims,
		Queries: len(raw.Queries),
		K:       *workK,
		Config: map[string]any{
			"maxConnections": uc.MaxConnections, "efConstruction": uc.EFConstruction,
			"trainingLimit": uc.RQ.TrainingLimit, "quantization": "rq bits=1 centering=true",
			"distance": uc.Distance,
		},
	}
	result.Levels, result.PackedConnectionBytes = graphLevels(index)
	t.Logf("graph: %+v, %d packed connection bytes", result.Levels, result.PackedConnectionBytes)
	writeWorkResult(t, result)

	counts := &workCompressor{VectorCompressor: index.compressor}
	index.compressActionLock.Lock()
	index.compressor = counts
	index.compressActionLock.Unlock()

	// One query at a time, with nothing fanned out, so that the counters
	// only ever see the query being counted.
	queryCtx := concurrency.CtxWithBudget(ctx, 1)
	for _, rescore := range workInts(t, *workRescores) {
		for _, ef := range workInts(t, *workEFs) {
			uc.EF, uc.RQ.RescoreLimit = ef, rescore
			updated := make(chan struct{})
			require.NoError(t, index.UpdateUserConfig(uc, func() { close(updated) }))
			<-updated

			counts.toCodes.Store(0)
			counts.toVectors.Store(0)
			hits, total := 0, 0
			for i, query := range raw.Queries {
				ids, _, err := index.SearchByVector(queryCtx, query, *workK, nil)
				require.NoError(t, err)
				for _, want := range truth[i][:*workK] {
					if slices.Contains(ids, want) {
						hits++
					}
				}
				total += *workK
			}

			point := workPoint{
				EF:              ef,
				RescoreLimit:    rescore,
				Recall:          float64(hits) / float64(total),
				CodeDistances:   float64(counts.toCodes.Load()) / float64(len(raw.Queries)),
				VectorDistances: float64(counts.toVectors.Load()) / float64(len(raw.Queries)),
			}
			result.Points = append(result.Points, point)
			t.Logf("ef=%d rescore=%d recall=%.4f distances per query: %.0f to codes, %.0f to vectors",
				ef, rescore, point.Recall, point.CodeDistances, point.VectorDistances)
			writeWorkResult(t, result)
		}
	}
}

// graphLevels counts the nodes and the connections of every level, and the
// serialized size of all connection lists.
func graphLevels(index *hnsw) ([]workLevel, int64) {
	index.RLock()
	defer index.RUnlock()

	var levels []workLevel
	var packed int64
	for _, node := range index.nodes {
		if node == nil {
			continue
		}
		node.Lock()
		for level := 0; level < int(node.connections.Layers()); level++ {
			for len(levels) <= level {
				levels = append(levels, workLevel{Level: len(levels)})
			}
			levels[level].Nodes++
			levels[level].Connections += node.connections.LenAtLayer(uint8(level))
		}
		packed += int64(len(node.connections.Data()))
		node.Unlock()
	}
	return levels, packed
}

func writeWorkResult(t *testing.T, result *workResult) {
	if *workOut == "" {
		return
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*workOut, raw, 0o644))
}

func workParallel(n int, fn func(i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
}

func workInts(t *testing.T, list string) []int {
	var out []int
	for _, field := range strings.Split(list, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoErrorf(t, err, "not a list of integers: %q", list)
		out = append(out, value)
	}
	return out
}
