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

package postingedge

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

// The measure of filters on the integrated index (bench-report.md of the
// experiments, "Filtros en el índice integrado, 1M dbpedia"; integration-plan.md,
// section 8): synthetic filters at four shares and of three kinds, their
// ground truth by brute force, the curves of recall over ef of the index on
// its two paths and of HNSW, untimed, and then, in the same process, timed
// rounds of every filter at its point of 0.95, warm and cold.

var (
	benchFiltersIndex = flag.String("bench.filters.index", "", "TestIntegrationFilters: the directory of the "+
		"integrated index the scale harness left, grown over the whole train set, compacted and laid out")
	benchFiltersHNSW = flag.String("bench.filters.hnswdir", "", "TestIntegrationFilters: the directory, on the "+
		"disk under test, of the file of the vectors HNSW rescores with, written there if it is not; empty for no HNSW")
	benchFiltersCache = flag.String("bench.filters.cache", "", "TestIntegrationFilters: the file of the filters and "+
		"their ground truth, written if it is not there")
	benchFiltersEFs = flag.String("bench.filters.efs", "10,20,40,80,160,320,640,1024,2048",
		"TestIntegrationFilters: the grid of ef of the untimed curves")
	benchFiltersRounds = flag.Int("bench.filters.rounds", 0, "TestIntegrationFilters: the timed rounds after the "+
		"curves, 0 for none")
	benchFiltersWait = flag.String("bench.filters.wait", "", "TestIntegrationFilters: the shell command run before "+
		"the timed rounds, which returns when the machine may be timed and fails if it may not")
	benchFiltersSweeping = flag.Bool("bench.filters.sweeping", true, "TestIntegrationFilters: HNSW with sweeping "+
		"too, after HNSW with acorn, its default: its curves, and its timed rounds after all the others")
	benchFiltersUntil = flag.String("bench.filters.until", "", "TestIntegrationFilters: the time of day, HH:MM, "+
		"after which no timed point starts; empty for none. The points it leaves out are told")
	benchFiltersSearchers = flag.String("bench.filters.searchers", "", "TestIntegrationFilters: the searchers to "+
		"measure, by index and path (ours graph, ours flat, hnsw acorn, hnsw sweeping), comma separated; all of them "+
		"when empty")
	benchFiltersOnly = flag.String("bench.filters.only", "", "TestIntegrationFilters: the filters to measure, by "+
		"kind (none, random, near, far), comma separated; empty for all")
	benchFiltersPoints = flag.String("bench.filters.points", "", "TestIntegrationFilters: the only points to "+
		"measure, each a searcher and a filter by name separated by a colon, comma separated, as in "+
		"\"ours graph:random 1 %,hnsw acorn:far 0.1 %\": the repeat of the points the drift detector marks. "+
		"Empty for every searcher on every filter")
	benchFiltersProbe = flag.String("bench.filters.probe", "", "TestIntegrationFilters: a diagnostic, never a "+
		"measure: one point, named as in -bench.filters.points, searched warm and alone before the curves, after "+
		"them, after every timed point and at the end, with the memory and the GC of the process and a CPU profile "+
		"each time, to see whether the process gets slower as it works. Empty for none")
	benchFiltersProbeEF      = flag.Int("bench.filters.probeef", 10, "TestIntegrationFilters: the ef of the probe")
	benchFiltersProbeQueries = flag.Int("bench.filters.probequeries", 300, "TestIntegrationFilters: the queries of "+
		"the probe, the first of the set")
	benchFiltersProbeProfiles = flag.String("bench.filters.probeprofiles", "", "TestIntegrationFilters: the "+
		"directory of the CPU profiles of the probe, one a probe; empty for none")
	benchFiltersUntimedProfiles = flag.String("bench.filters.untimedprofiles", "", "TestIntegrationFilters: the "+
		"directory of a CPU profile of the untimed pass of every timed point, the one that checks its recall before "+
		"the warm passes, which are never profiled; empty for none")
	benchFiltersExtraRandom = flag.String("bench.filters.extrarandom", "", "TestIntegrationFilters: more shares at "+
		"random, in percent, comma separated, as in \"7,5,3\": filters drawn by the same code as the four of the "+
		"measure and a seed of their own, measured after the others; read from the cache, or drawn and added to it "+
		"after the filters of the measure, which stay as they are. Filters of the cache beyond the measure that are "+
		"not named here are not measured")
	benchFiltersTimed = flag.String("bench.filters.timed", "", "TestIntegrationFilters: the searchers the timed "+
		"rounds time, by index and path, comma separated, in their order of always: the others still find their "+
		"curves, untimed, and HNSW stays alive to the end. Empty for every searcher measured")
	benchFiltersCacheOnly = flag.Bool("bench.filters.cacheonly", false, "TestIntegrationFilters: stop once the "+
		"filters are read or drawn and the cache is written, before the indexes are opened: to extend the cache "+
		"ahead of a timed session")
	benchFiltersCacheFixed = flag.Bool("bench.filters.cachefixed", false, "TestIntegrationFilters: fail unless the "+
		"cache holds every filter measured, so that a timed session never draws one")
	benchFiltersHNSWCentering = flag.Bool("bench.filters.hnswcentering", false, "TestIntegrationFilters: HNSW with "+
		"its 1-bit RQ centered (rqCentering), the cell of the 10M table, instead of the default of a class")
	benchFiltersMemory = flag.Bool("bench.filters.memory", false, "TestIntegrationFilters: the memory of the "+
		"system, untimed, before and after the warm passes and the cold batches of every timed point: vm_stat, "+
		"memory_pressure -Q, sysctl vm.swapusage and top on the process (macOS), in the events and the JSON")
)

// filterSearcherNames are the searchers of the measure, by index and path.
var filterSearcherNames = []string{"ours graph", "ours flat", "hnsw acorn", "hnsw sweeping"}

// filterPoint is one point of -bench.filters.points: a searcher, by index and
// path, on a filter, by name.
type filterPoint struct{ searcher, filter string }

// parseFilterPoints reads -bench.filters.points against the names of the
// searchers and of the filters, and refuses a name that is neither, so that a
// misspelt point is not left out of a measure without a word.
func parseFilterPoints(spec string, searchers, filters []string) ([]filterPoint, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	var points []filterPoint
	for _, item := range strings.Split(spec, ",") {
		searcher, filter, ok := strings.Cut(item, ":")
		searcher, filter = strings.TrimSpace(searcher), strings.TrimSpace(filter)
		if !ok || searcher == "" || filter == "" {
			return nil, fmt.Errorf("point %q is not a searcher and a filter separated by a colon", item)
		}
		if !slices.Contains(searchers, searcher) {
			return nil, fmt.Errorf("point %q: no searcher is named %q, they are %q", item, searcher, searchers)
		}
		if !slices.Contains(filters, filter) {
			return nil, fmt.Errorf("point %q: no filter is named %q, they are %q", item, filter, filters)
		}
		if slices.Contains(points, filterPoint{searcher, filter}) {
			return nil, fmt.Errorf("point %q is named twice", item)
		}
		points = append(points, filterPoint{searcher, filter})
	}
	return points, nil
}

// The shares and the kinds of filter of the measure.
var (
	filterShares = []float64{0.5, 0.1, 0.01, 0.001}
	filterKinds  = []string{"random", "near", "far"}
)

// filterTargets are the recalls whose points the curves look for, and the
// first of them is the one the timed rounds take.
var filterTargets = []float64{0.95, 0.99}

// benchFilter is one filter of the measure: a set of allowed doc ids for all
// the queries (random), or one for every query (near and far), and the k
// allowed doc ids nearest to every query.
type benchFilter struct {
	Name    string
	Kind    string
	Share   float64
	Allowed int
	allow   helpers.AllowList
	allows  []helpers.AllowList
	Truth   [][]uint64
}

// of is the filter of query q, nil for the measure without one.
func (f *benchFilter) of(q int) helpers.AllowList {
	if f.allows != nil {
		return f.allows[q]
	}
	return f.allow
}

func filterName(kind string, share float64) string {
	if kind == "none" {
		return "none"
	}
	// Ten digits, so that the percent of a share such as 0.07, which is
	// 7.000000000000001, reads 7.
	return fmt.Sprintf("%s %.10g %%", kind, 100*share)
}

// filterCache is what the cache file holds: every allow list as the buffer of
// its bitmap, by filter and, for a ball, by query, the pivots of the balls,
// and the ground truth of every filter.
type filterCache struct {
	Dataset string
	Vectors int
	Queries int
	K       int
	Names   []string
	Kinds   []string
	Shares  []float64
	// Bitmaps has one buffer for a filter at random, and one a query for a
	// ball; Pivots the pivot of every query of a ball.
	Bitmaps [][][]byte
	Pivots  [][]uint64
	Truth   [][][]uint64
}

// bitmapOf builds the allow list of a set of doc ids, sorted.
func bitmapOf(ids []uint64) *sroar.Bitmap {
	return sroar.FromSortedList(ids)
}

// buildFilters draws the filters of the measure over the train set, which is
// in memory and normalized, and finds their ground truth by brute force. The
// unfiltered measure is the first, with its own ground truth.
func buildFilters(t *testing.T, data *benchData, k int) []*benchFilter {
	n, q := len(data.train), len(data.queries)
	provider := distancer.NewCosineDistanceProvider()
	begin := time.Now()

	// At random: one set a share, the same for every query.
	random := make([][]bool, len(filterShares))
	randomIDs := make([][]uint64, len(filterShares))
	for s, share := range filterShares {
		ids := randomFilterIDs(n, share, 1000+uint64(s))
		randomIDs[s] = ids
		random[s] = make([]bool, n)
		for _, id := range ids {
			random[s][id] = true
		}
	}

	// A pass a query: its 1 000 nearest and farthest vectors, its ground
	// truth without a filter and within the filters at random.
	const around = 1000
	nearest, farthest := make([][]uint64, q), make([][]uint64, q)
	noneTruth := make([][]uint64, q)
	randomTruth := make([][][]uint64, len(filterShares))
	for s := range randomTruth {
		randomTruth[s] = make([][]uint64, q)
	}
	parallelFor(q, func(i int) {
		var near, far, none topK
		near.reset(around)
		far.reset(around)
		none.reset(k)
		within := make([]topK, len(filterShares))
		for s := range within {
			within[s].reset(k)
		}
		for id, vec := range data.train {
			dist, err := provider.SingleDist(data.queries[i], vec)
			if err != nil {
				panic(err)
			}
			near.push(uint64(id), dist)
			far.push(uint64(id), -dist)
			none.push(uint64(id), dist)
			for s := range within {
				if random[s][id] {
					within[s].push(uint64(id), dist)
				}
			}
		}
		nearest[i], farthest[i] = idsOf(near.sorted()), idsOf(far.sorted())
		noneTruth[i] = idsOf(none.sorted())
		for s := range within {
			randomTruth[s][i] = idsOf(within[s].sorted())
		}
	})
	t.Logf("filters: the pass over the queries in %s", time.Since(begin).Round(time.Second))

	filters := []*benchFilter{{Name: "none", Kind: "none", Allowed: n, Truth: noneTruth}}
	for s, share := range filterShares {
		filters = append(filters, &benchFilter{
			Name: filterName("random", share), Kind: "random", Share: share, Allowed: len(randomIDs[s]),
			allow: helpers.NewAllowListFromBitmap(bitmapOf(randomIDs[s])), Truth: randomTruth[s],
		})
	}

	// The balls: for every query a pivot at random among its 1 000 nearest
	// (near) or farthest (far) vectors, a seed a kind and share, and the
	// share of the vectors nearest to the pivot, with their ground truth.
	for kindAt, kind := range filterKinds[1:] {
		around := nearest
		if kind == "far" {
			around = farthest
		}
		for s, share := range filterShares {
			rng := rand.New(rand.NewPCG(2000+uint64(10*kindAt+s), 11))
			pivots := make([]uint64, q)
			for i := range pivots {
				pivots[i] = around[i][rng.IntN(len(around[i]))]
			}
			m := int(share * float64(n))
			filter := &benchFilter{
				Name: filterName(kind, share), Kind: kind, Share: share, Allowed: m,
				allows: make([]helpers.AllowList, q), Truth: make([][]uint64, q),
			}
			buffers := sync.Pool{New: func() any {
				buf := make([]scored, n)
				return &buf
			}}
			parallelFor(q, func(i int) {
				buf := buffers.Get().(*[]scored)
				defer buffers.Put(buf)
				all := *buf
				pivot := data.train[pivots[i]]
				for id, vec := range data.train {
					dist, err := provider.SingleDist(pivot, vec)
					if err != nil {
						panic(err)
					}
					all[id] = scored{id: uint64(id), dist: dist}
				}
				slices.SortFunc(all, func(a, b scored) int {
					switch {
					case worse(b, a):
						return -1
					case worse(a, b):
						return 1
					}
					return 0
				})
				ids := make([]uint64, m)
				var best topK
				best.reset(k)
				for j := range ids {
					ids[j] = all[j].id
					dist, err := provider.SingleDist(data.queries[i], data.train[ids[j]])
					if err != nil {
						panic(err)
					}
					best.push(ids[j], dist)
				}
				slices.Sort(ids)
				filter.allows[i] = helpers.NewAllowListFromBitmap(bitmapOf(ids))
				filter.Truth[i] = idsOf(best.sorted())
			})
			filters = append(filters, filter)
			t.Logf("filters: %s in %s", filter.Name, time.Since(begin).Round(time.Second))
		}
	}
	return filters
}

func idsOf(sorted []scored) []uint64 {
	ids := make([]uint64, len(sorted))
	for i, s := range sorted {
		ids[i] = s.id
	}
	return ids
}

// writeFilterCache and readFilterCache keep the filters in a file, so that
// the run that is timed does not draw them again.
func writeFilterCache(t *testing.T, path string, data *benchData, k int, filters []*benchFilter) {
	c := filterCache{Dataset: data.name, Vectors: data.rows, Queries: len(data.queries), K: k}
	for _, f := range filters {
		c.Names, c.Kinds, c.Shares = append(c.Names, f.Name), append(c.Kinds, f.Kind), append(c.Shares, f.Share)
		c.Truth = append(c.Truth, f.Truth)
		var buffers [][]byte
		switch {
		case f.allows != nil:
			for _, allow := range f.allows {
				buffers = append(buffers, allow.(*helpers.BitmapAllowList).Bm.ToBuffer())
			}
		case f.allow != nil:
			buffers = append(buffers, f.allow.(*helpers.BitmapAllowList).Bm.ToBuffer())
		}
		c.Bitmaps = append(c.Bitmaps, buffers)
	}
	file, err := os.Create(path + ".tmp")
	require.NoError(t, err)
	require.NoError(t, gob.NewEncoder(file).Encode(&c))
	require.NoError(t, file.Close())
	require.NoError(t, os.Rename(path+".tmp", path))
}

func readFilterCache(t *testing.T, path string, data *benchData, k int) ([]*benchFilter, bool) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, false
	}
	require.NoError(t, err)
	defer file.Close()
	var c filterCache
	require.NoError(t, gob.NewDecoder(file).Decode(&c))
	require.Equal(t, data.name, c.Dataset, "the cache is of another dataset")
	require.Equal(t, data.rows, c.Vectors)
	require.Equal(t, len(data.queries), c.Queries)
	require.Equal(t, k, c.K)
	filters := make([]*benchFilter, len(c.Names))
	for i := range filters {
		f := &benchFilter{Name: c.Names[i], Kind: c.Kinds[i], Share: c.Shares[i], Truth: c.Truth[i]}
		switch f.Kind {
		case "random":
			require.Len(t, c.Bitmaps[i], 1, f.Name)
			f.allow = helpers.NewAllowListFromBitmap(sroar.FromBufferWithCopy(c.Bitmaps[i][0]))
			f.Allowed = f.allow.Len()
		case "near", "far":
			require.Len(t, c.Bitmaps[i], c.Queries, f.Name)
			for _, buffer := range c.Bitmaps[i] {
				f.allows = append(f.allows, helpers.NewAllowListFromBitmap(sroar.FromBufferWithCopy(buffer)))
			}
			f.Allowed = f.allows[0].Len()
		default:
			f.Allowed = data.rows
		}
		filters[i] = f
	}
	return filters, true
}

// filterCurvePoint is what a curve measures at one ef, per query.
type filterCurvePoint struct {
	EF     int     `json:"ef"`
	Recall float64 `json:"recall"`
	// Loads, Rounds, Scored, StoreReads and StoreBytes are of the
	// integrated index: postings loaded, codes ranked by a flat search, and
	// what the store read; Flat is the share of the queries it searched
	// flat. VectorReads is the exact vectors read, in both indexes.
	Loads       float64 `json:"loads,omitempty"`
	Rounds      float64 `json:"rounds,omitempty"`
	Scored      float64 `json:"scored,omitempty"`
	StoreReads  float64 `json:"storeReads,omitempty"`
	StoreBytes  float64 `json:"storeBytes,omitempty"`
	Flat        float64 `json:"flat,omitempty"`
	VectorReads float64 `json:"vectorReads"`
	// Seconds is what the 8 queries at a time took, all together. Not a
	// timing: the machine is not alone.
	Seconds float64 `json:"seconds"`
}

// filterCurve is the curve of one index on one path with one filter.
type filterCurve struct {
	Index   string             `json:"index"`
	Path    string             `json:"path"`
	Filter  string             `json:"filter"`
	Kind    string             `json:"kind"`
	Share   float64            `json:"share"`
	Allowed int                `json:"allowed"`
	Grid    []filterCurvePoint `json:"grid"`
	// Points are the smallest ef that reach each target, found between two
	// values of the grid, with what a query does there; nil where the grid
	// does not reach it.
	Points []*filterCurvePoint `json:"points"`
	Best   float64             `json:"bestRecall"`
}

// filterTimed is one point timed in one round.
type filterTimed struct {
	Index  string  `json:"index"`
	Path   string  `json:"path"`
	Filter string  `json:"filter"`
	Target float64 `json:"target"`
	EF     int     `json:"ef"`
	Recall float64 `json:"recall"`
	// Warm is every timed pass over all the queries, after an untimed one,
	// with 1 thread; Reads the exact vectors read a query and Loads the
	// postings loaded, in the warm passes.
	Warm  []diskLatencies `json:"warm"`
	Reads float64         `json:"reads"`
	Loads float64         `json:"loads,omitempty"`
	// Cold is the cold queries together, and ColdHalves the two halves of
	// the batches apart.
	Cold        *diskLatencies  `json:"cold,omitempty"`
	ColdHalves  []diskLatencies `json:"coldHalves,omitempty"`
	ColdBatches int             `json:"coldBatches,omitempty"`
	ColdBatch   int             `json:"coldBatch,omitempty"`
	ColdRecall  float64         `json:"coldRecall,omitempty"`
	ReopenMs    float64         `json:"reopenms,omitempty"`
	// Memory is the memory of the system around the point
	// (-bench.filters.memory), untimed.
	Memory *filterMemory `json:"memory,omitempty"`
}

// filterMemory is the memory of the system around a timed point: before its
// untimed pass, before its first warm pass, after its last, and before and
// after its cold batches.
type filterMemory struct {
	BeforeUntimed systemMemory  `json:"beforeUntimed"`
	BeforeWarm    systemMemory  `json:"beforeWarm"`
	AfterWarm     systemMemory  `json:"afterWarm"`
	BeforeCold    *systemMemory `json:"beforeCold,omitempty"`
	AfterCold     *systemMemory `json:"afterCold,omitempty"`
}

// filterRound is one timed round. The rounds of HNSW with sweeping come after
// those of the others and are numbered after them. Skipped is the points it
// left out because it was past -bench.filters.until.
type filterRound struct {
	Round   int           `json:"round"`
	Start   string        `json:"start"`
	End     string        `json:"end"`
	Points  []filterTimed `json:"points"`
	Skipped []string      `json:"skipped,omitempty"`
}

type filterResult struct {
	Dataset   string         `json:"dataset"`
	Vectors   int            `json:"vectors"`
	Dims      int            `json:"dims"`
	Queries   int            `json:"queries"`
	K         int            `json:"k"`
	Meta      growMeta       `json:"meta"`
	Cutoff    int            `json:"flatCutoff"`
	HNSW      map[string]any `json:"hnsw,omitempty"`
	Admission bool           `json:"admission,omitempty"`
	Budget    int            `json:"budget"`
	Procs     int            `json:"gomaxprocs"`
	Drop      string         `json:"drop"`
	// FilterSeconds is what drawing the filters took, 0 if they came from
	// the cache, and HNSWBuildSeconds what building HNSW took.
	FilterSeconds    float64  `json:"filterSeconds"`
	HNSWBuildSeconds float64  `json:"hnswBuildSeconds,omitempty"`
	Filters          []string `json:"filters"`
	Allowed          []int    `json:"allowed"`
	// Timed is the searchers the timed rounds time (-bench.filters.timed),
	// empty when they time every searcher measured.
	Timed  []string      `json:"timed,omitempty"`
	Curves []filterCurve `json:"curves"`
	Rounds []filterRound `json:"rounds"`
}

// filterSearcher is one index on one path, as the measure searches it.
type filterSearcher struct {
	index, path string
	// setEF sets the ef of the searches, prepare readies the path, and
	// search answers one query with the filter of it, and tells what it
	// did.
	setEF   func(ef int)
	prepare func()
	search  func(ctx context.Context, q int, allow helpers.AllowList) ([]uint64, diskSearchStats, error)
	// reads, when set, counts the exact vectors the index has read so far.
	reads func() int64
	// close and open let the cache under the index be dropped.
	close, open func()
}

// filterOpenSet opens one searcher at a time over the disk under test, and
// closes before a drop of the cache every searcher in built.
type filterOpenSet struct {
	built  []*filterSearcher
	opened *filterSearcher
}

func (o *filterOpenSet) closeAll() {
	for _, s := range o.built {
		if s.close != nil {
			s.close()
		}
	}
	o.opened = nil
}

// openOnly closes every searcher and opens s, unless s is the one open, and
// tells how long that took.
func (o *filterOpenSet) openOnly(s *filterSearcher) time.Duration {
	clock := time.Now()
	if o.opened != s {
		o.closeAll()
		s.open()
		o.opened = s
	}
	return time.Since(clock)
}

// measuredFilterSearchers keeps, of the searchers built, those names names
// (every one when it is empty) and pointed measures, and gives the set that
// opens them one at a time over the disk under test.
func measuredFilterSearchers(built []*filterSearcher, names string, pointed func(searcher, filter string) bool,
) ([]*filterSearcher, *filterOpenSet, error) {
	measured := slices.Clone(built)
	if names != "" {
		wanted := map[string]bool{}
		for _, name := range strings.Split(names, ",") {
			wanted[strings.TrimSpace(name)] = true
		}
		measured = slices.DeleteFunc(measured, func(s *filterSearcher) bool { return !wanted[s.index+" "+s.path] })
		if len(measured) == 0 {
			return nil, nil, fmt.Errorf("no searcher is named %q", names)
		}
	}
	measured = slices.DeleteFunc(measured, func(s *filterSearcher) bool { return !pointed(s.index+" "+s.path, "") })
	return measured, &filterOpenSet{built: measured}, nil
}

// TestIntegrationFilters measures filters on the integrated index and on
// HNSW at 1M (see the head of this file). The integrated index is the one the
// scale harness left in -bench.filters.index; HNSW is built here, with its
// vectors in a file of -bench.filters.hnswdir read with pread to rescore.
func TestIntegrationFilters(t *testing.T) {
	if *benchDatasetDir == "" || *benchFiltersIndex == "" {
		t.Skip("no -bench.dataset and -bench.filters.index given")
	}
	require.NotEmpty(t, *benchOut, "TestIntegrationFilters writes its results to -bench.out")
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	k := *benchK
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, k, false)
	n, queries := len(data.train), data.queries
	efs := parseInts(t, *benchFiltersEFs)

	result := &filterResult{
		Dataset: data.name, Vectors: n, Dims: data.dims, Queries: len(queries), K: k,
		Admission: *benchDiskAdmission, Budget: *benchDiskBudget, Procs: runtime.GOMAXPROCS(0), Drop: *benchDiskDrop,
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}

	// The filters, drawn or read from the cache.
	begin := time.Now()
	var filters []*benchFilter
	cached := false
	if *benchFiltersCache != "" {
		filters, cached = readFilterCache(t, *benchFiltersCache, data, k)
	}
	require.False(t, *benchFiltersCacheFixed && !cached, "-bench.filters.cachefixed: no cache at %q", *benchFiltersCache)
	if !cached {
		filters = buildFilters(t, data, k)
		result.FilterSeconds = time.Since(begin).Seconds()
		if *benchFiltersCache != "" {
			writeFilterCache(t, *benchFiltersCache, data, k, filters)
		}
	}
	extra, err := parseExtraShares(*benchFiltersExtraRandom)
	require.NoError(t, err, "-bench.filters.extrarandom")
	filters, drawn := withExtraRandom(data, k, filters, extra)
	require.False(t, *benchFiltersCacheFixed && drawn, "-bench.filters.cachefixed: the cache lacks a filter of "+
		"-bench.filters.extrarandom %q", *benchFiltersExtraRandom)
	if drawn && *benchFiltersCache != "" {
		// The filters of the measure come from the cache as they were: the
		// cache written again must give back every filter as it is here.
		writeFilterCache(t, *benchFiltersCache, data, k, filters)
		written, ok := readFilterCache(t, *benchFiltersCache, data, k)
		require.True(t, ok)
		require.Len(t, written, len(filters))
		for i := range filters {
			requireSameFilter(t, filters[i], written[i], len(data.queries))
		}
	}
	if *benchFiltersCacheOnly {
		for _, f := range filters {
			t.Logf("filters, %s: %d allowed", f.Name, f.Allowed)
		}
		t.Logf("filters: %d, and nothing else (-bench.filters.cacheonly)", len(filters))
		return
	}
	if *benchFiltersOnly != "" {
		wanted := map[string]bool{}
		for _, kind := range strings.Split(*benchFiltersOnly, ",") {
			wanted[kind] = true
		}
		filters = slices.DeleteFunc(filters, func(f *benchFilter) bool { return !wanted[f.Kind] })
	}
	names := make([]string, len(filters))
	for i, f := range filters {
		names[i] = f.Name
	}
	points, err := parseFilterPoints(*benchFiltersPoints, filterSearcherNames, names)
	require.NoError(t, err, "-bench.filters.points")
	// pointed tells whether a searcher, by index and path, measures a filter,
	// by name: always, unless -bench.filters.points names the points.
	pointed := func(searcher, filter string) bool {
		if points == nil {
			return true
		}
		return slices.ContainsFunc(points, func(p filterPoint) bool {
			return p.searcher == searcher && (filter == "" || p.filter == filter)
		})
	}
	filters = slices.DeleteFunc(filters, func(f *benchFilter) bool {
		return points != nil && !slices.ContainsFunc(points, func(p filterPoint) bool { return p.filter == f.Name })
	})
	for _, f := range filters {
		result.Filters, result.Allowed = append(result.Filters, f.Name), append(result.Allowed, f.Allowed)
		require.Len(t, f.Truth, len(queries), f.Name)
	}
	t.Logf("filters: %d, %s, in %s", len(filters), map[bool]string{true: "from the cache", false: "drawn"}[cached],
		time.Since(begin).Round(time.Second))
	write()

	// The budget of a query: fixed, or what the limiter of a node grants it.
	var limiter *queryadmission.Limiter
	if *benchDiskAdmission {
		limiter = queryadmission.New(prometheus.NewRegistry(), queryadmission.Config{})
	}
	budgeted := func() (context.Context, func(), error) {
		if limiter == nil {
			return concurrency.CtxWithBudget(ctx, *benchDiskBudget), func() {}, nil
		}
		return limiter.Admit(ctx, concurrency.TimesGOMAXPROCS(2))
	}

	ours := openFilteredIndex(t, ctx, logger, *benchFiltersIndex)
	defer ours.shut()
	result.Meta = ours.p.g.meta
	result.Cutoff = DefaultFlatSearchCutoff
	built := []*filterSearcher{ours.searcher("graph", -1, queries, k), ours.searcher("flat", math.MaxInt, queries, k)}
	var hnswIndex *filterHNSW
	if *benchFiltersHNSW != "" {
		hnswIndex = buildFilterHNSW(t, ctx, logger, data, *benchFiltersHNSW)
		defer hnswIndex.shut(ctx)
		result.HNSW, result.HNSWBuildSeconds = hnswIndex.config, hnswIndex.buildSeconds
		// HNSW with acorn, its default, is the comparison the measure reads;
		// sweeping comes after it (amendment 1 of the pre-registration).
		built = append(built, hnswIndex.searcher(t, enthnsw.FilterStrategyAcorn, queries, k))
		if *benchFiltersSweeping {
			built = append(built, hnswIndex.searcher(t, enthnsw.FilterStrategySweeping, queries, k))
		}
	}
	searchers, onDisk, err := measuredFilterSearchers(built, *benchFiltersSearchers, pointed)
	require.NoError(t, err, "-bench.filters.searchers")
	for _, p := range points {
		require.True(t, slices.ContainsFunc(searchers, func(s *filterSearcher) bool { return s.index+" "+s.path == p.searcher }),
			"point %s on %s: the searcher is not measured (no HNSW, or left out by another flag)", p.searcher, p.filter)
	}
	measured := make([]string, len(searchers))
	for i, s := range searchers {
		measured[i] = s.index + " " + s.path
	}
	timedOnly, err := parseTimedSearchers(*benchFiltersTimed, filterSearcherNames, measured)
	require.NoError(t, err, "-bench.filters.timed")
	for _, name := range measured {
		if timedOnly[name] {
			result.Timed = append(result.Timed, name)
		}
	}
	write()

	// One query, checked: no doc id the filter leaves out, ever.
	check := func(s *filterSearcher, f *benchFilter, q int, ids []uint64) int {
		allow := f.of(q)
		for _, id := range ids {
			if allow != nil && !allow.Contains(id) {
				require.FailNowf(t, "STOP", "%s %s, %s, query %d: doc %d is left out by the filter", s.index, s.path,
					f.Name, q, id)
			}
		}
		hits := 0
		for _, want := range f.Truth[q] {
			if slices.Contains(ids, want) {
				hits++
			}
		}
		return hits
	}

	// measure searches every query, 8 at a time with a budget of 1, untimed.
	measure := func(s *filterSearcher, f *benchFilter, ef int) filterCurvePoint {
		s.setEF(ef)
		point := filterCurvePoint{EF: ef}
		var hits, flat, loads, rounds, scored atomic.Int64
		readsBefore, bytesBefore, err := diskStoreReads()
		require.NoError(t, err)
		var vectorsBefore int64
		if s.reads != nil {
			vectorsBefore = s.reads()
		}
		var rescored atomic.Int64
		clock := time.Now()
		searchCtx := concurrency.CtxWithBudget(ctx, 1)
		err = parallelChunks(ctx, logger, len(queries), 8, func(_, start, end int) error {
			for q := start; q < end; q++ {
				ids, stats, err := s.search(searchCtx, q, f.of(q))
				if err != nil {
					return err
				}
				hits.Add(int64(check(s, f, q, ids)))
				loads.Add(int64(stats.Postings))
				rounds.Add(int64(stats.Rounds))
				scored.Add(int64(stats.Scored))
				rescored.Add(int64(stats.Rescored))
				if stats.Flat {
					flat.Add(1)
				}
			}
			return nil
		})
		require.NoError(t, err)
		point.Seconds = time.Since(clock).Seconds()
		readsAfter, bytesAfter, err := diskStoreReads()
		require.NoError(t, err)
		nq := float64(len(queries))
		point.Recall = float64(hits.Load()) / float64(len(queries)*k)
		point.Loads, point.Rounds = float64(loads.Load())/nq, float64(rounds.Load())/nq
		point.Scored, point.Flat = float64(scored.Load())/nq, float64(flat.Load())/nq
		if s.index == "ours" {
			point.StoreReads, point.StoreBytes = float64(readsAfter-readsBefore)/nq, float64(bytesAfter-bytesBefore)/nq
			point.VectorReads = float64(rescored.Load()) / nq
		} else {
			point.VectorReads = float64(s.reads()-vectorsBefore) / nq
		}
		return point
	}

	// probe is a diagnostic, never a measure (-bench.filters.probe): one
	// searcher on one filter, searched warm and alone with the budget of a
	// node, then the allow lists of its queries iterated alone, with the
	// memory and the GC of the process at that moment and a CPU profile of
	// the searches. It tells whether the process gets slower as it works,
	// and where. The caller has the index of the probe open.
	var probeSearcher *filterSearcher
	var probeFilter *benchFilter
	if *benchFiltersProbe != "" {
		named, err := parseFilterPoints(*benchFiltersProbe, filterSearcherNames, names)
		require.NoError(t, err, "-bench.filters.probe")
		require.Len(t, named, 1, "-bench.filters.probe names one point")
		for _, s := range searchers {
			if s.index+" "+s.path == named[0].searcher {
				probeSearcher = s
			}
		}
		for _, f := range filters {
			if f.Name == named[0].filter {
				probeFilter = f
			}
		}
		require.NotNil(t, probeSearcher, "-bench.filters.probe: %s is not measured", named[0].searcher)
		require.NotNil(t, probeFilter, "-bench.filters.probe: %s is not measured", named[0].filter)
	}
	probes, gcsBefore := 0, uint32(0)
	probe := func(label string) {
		if probeSearcher == nil {
			return
		}
		probes++
		s, f, n := probeSearcher, probeFilter, min(*benchFiltersProbeQueries, len(queries))
		s.prepare()
		s.setEF(*benchFiltersProbeEF)
		search := func() []time.Duration {
			latencies := make([]time.Duration, n)
			for q := range n {
				searchCtx, release, err := budgeted()
				require.NoError(t, err)
				clock := time.Now()
				ids, _, err := s.search(searchCtx, q, f.of(q))
				latencies[q] = time.Since(clock)
				release()
				require.NoError(t, err)
				check(s, f, q, ids)
			}
			return latencies
		}
		search()
		profile := "none"
		var file *os.File
		if *benchFiltersProbeProfiles != "" {
			profile = filepath.Join(*benchFiltersProbeProfiles, fmt.Sprintf("probe-%02d.pprof", probes))
			var err error
			file, err = os.Create(profile)
			require.NoError(t, err)
			require.NoError(t, pprof.StartCPUProfile(file))
		}
		latencies := search()
		if file != nil {
			pprof.StopCPUProfile()
			require.NoError(t, file.Close())
		}
		var iterate time.Duration
		members := 0
		for q := range n {
			allow := f.of(q)
			if allow == nil {
				continue
			}
			clock := time.Now()
			it := allow.Iterator()
			for _, ok := it.Next(); ok; _, ok = it.Next() {
				members++
			}
			it.Stop()
			iterate += time.Since(clock)
		}
		var total time.Duration
		for _, l := range latencies {
			total += l
		}
		slices.Sort(latencies)
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		t.Logf("probe %d, %s: %s %s, %s (ef %d), %d queries alone, untimed: p50 %.3f ms, mean %.3f | iterating the "+
			"allow lists alone %.1f ns a member | heap in use %.0f MB, %d objects, %d GCs (%d since the last probe), GC "+
			"CPU fraction %.4f, %d goroutines | profile %s", probes, label, s.index, s.path, f.Name,
			*benchFiltersProbeEF, n, ms(latencies[n/2]), ms(total)/float64(n),
			float64(iterate.Nanoseconds())/float64(max(members, 1)), float64(mem.HeapInuse)/(1<<20), mem.HeapObjects,
			mem.NumGC, mem.NumGC-gcsBefore, mem.GCCPUFraction, runtime.NumGoroutine(), profile)
		gcsBefore = mem.NumGC
	}
	probe("at open, before the curves")

	// The curves: the grid, and the points of the targets between two of
	// its values. A curve stops once its recall is 0.999 or more.
	curves := map[string]*filterCurve{}
	key := func(s *filterSearcher, f *benchFilter) string { return s.index + "/" + s.path + "/" + f.Name }
	for _, s := range searchers {
		s.prepare()
		for _, f := range filters {
			if f.Kind == "none" && s.path == "flat" || !pointed(s.index+" "+s.path, f.Name) {
				continue
			}
			curve := &filterCurve{Index: s.index, Path: s.path, Filter: f.Name, Kind: f.Kind, Share: f.Share, Allowed: f.Allowed}
			at := map[int]filterCurvePoint{}
			recallAt := func(ef int) float64 {
				if p, done := at[ef]; done {
					return p.Recall
				}
				p := measure(s, f, ef)
				at[ef] = p
				t.Logf("%s %s, %s, ef %d: recall %.4f | %.1f loads, %.1f vectors read, %.0f codes scored, flat %.2f | %.1f s",
					s.index, s.path, f.Name, ef, p.Recall, p.Loads, p.VectorReads, p.Scored, p.Flat, p.Seconds)
				return p.Recall
			}
			for _, ef := range efs {
				r := recallAt(ef)
				curve.Grid = append(curve.Grid, at[ef])
				curve.Best = max(curve.Best, r)
				if r >= 0.999 {
					break
				}
			}
			for _, target := range filterTargets {
				low := 0
				var found *filterCurvePoint
				for _, ef := range efs {
					if _, done := at[ef]; !done {
						break
					}
					if at[ef].Recall >= target {
						high := ef
						for high-low > 1 {
							middle := (low + high) / 2
							if middle < k || recallAt(middle) < target {
								low = middle
							} else {
								high = middle
							}
						}
						recallAt(high)
						p := at[high]
						found = &p
						break
					}
					low = ef
				}
				curve.Points = append(curve.Points, found)
				if found == nil {
					t.Logf("%s %s, %s: recall %.2f is not reached, the best is %.4f", s.index, s.path, f.Name, target, curve.Best)
				} else {
					t.Logf("%s %s, %s at recall %.2f: ef %d, recall %.4f, %.1f loads, %.1f vectors read", s.index, s.path,
						f.Name, target, found.EF, found.Recall, found.Loads, found.VectorReads)
				}
			}
			curves[key(s, f)] = curve
			result.Curves = append(result.Curves, *curve)
			write()
		}
	}
	acornRan(t, filters, curves, key)
	probe("after the curves")

	if *benchFiltersRounds == 0 {
		return
	}
	if *benchFiltersWait != "" {
		out, err := exec.CommandContext(ctx, "sh", "-c", *benchFiltersWait).CombinedOutput()
		t.Logf("the wait before the timed rounds: %s", out)
		require.NoErrorf(t, err, "the wait before the timed rounds: %s", out)
	}

	// run searches the given queries one after the other, with the budget of
	// a node, and tells the latency and the stats of each, the hits and the
	// time of all.
	run := func(s *filterSearcher, f *benchFilter, which []int) ([]time.Duration, []diskSearchStats, int, time.Duration) {
		latencies := make([]time.Duration, len(which))
		stats := make([]diskSearchStats, len(which))
		hits := 0
		start := time.Now()
		for i, q := range which {
			var before int64
			if s.reads != nil {
				before = s.reads()
			}
			queryBegin := time.Now()
			searchCtx, release, err := budgeted()
			require.NoError(t, err)
			ids, st, err := s.search(searchCtx, q, f.of(q))
			release()
			latencies[i] = time.Since(queryBegin)
			require.NoError(t, err)
			st.Budget = concurrency.BudgetFromCtx(searchCtx, 0)
			if s.reads != nil {
				st.Rescored = int(s.reads() - before)
			}
			stats[i] = st
			hits += check(s, f, q, ids)
		}
		return latencies, stats, hits, time.Since(start)
	}
	all := make([]int, len(queries))
	for i := range all {
		all[i] = i
	}
	// sampleMemory samples the memory of the system, untimed; the measure
	// fails if macOS does not tell it.
	sampleMemory := func() systemMemory {
		m, err := sampleSystemMemory(ctx)
		require.NoError(t, err, "-bench.filters.memory")
		return m
	}
	mean := func(stats []diskSearchStats, of func(diskSearchStats) int) float64 {
		total := 0
		for _, s := range stats {
			total += of(s)
		}
		return float64(total) / float64(len(stats))
	}

	// Every searcher closes and opens what lies under it on the disk under
	// test; before a drop of the cache everything is closed, and only the
	// searcher that is timed is opened again.
	closeAll, openOnly := onDisk.closeAll, onDisk.openOnly

	// The time after which no timed point starts.
	var until time.Time
	if *benchFiltersUntil != "" {
		at, err := time.ParseInLocation("15:04", *benchFiltersUntil, time.Local)
		require.NoError(t, err)
		now := time.Now()
		until = time.Date(now.Year(), now.Month(), now.Day(), at.Hour(), at.Minute(), 0, 0, time.Local)
		if until.Before(now) {
			until = until.Add(24 * time.Hour)
		}
	}
	// The rounds of the integrated index and of HNSW with acorn first, and
	// those of HNSW with sweeping after all of them.
	// -bench.filters.timed leaves some out of the rounds, not out of the
	// curves.
	untimed := func(s *filterSearcher) bool { return timedOnly != nil && !timedOnly[s.index+" "+s.path] }
	main := slices.DeleteFunc(slices.Clone(searchers), func(s *filterSearcher) bool {
		return s.path == enthnsw.FilterStrategySweeping || untimed(s)
	})
	after := slices.DeleteFunc(slices.Clone(searchers), func(s *filterSearcher) bool {
		return s.path != enthnsw.FilterStrategySweeping || untimed(s)
	})
	type timedPass struct {
		round     int
		searchers []*filterSearcher
	}
	var passes []timedPass
	for round := 1; round <= *benchFiltersRounds; round++ {
		passes = append(passes, timedPass{round, main})
	}
	for round := 1; round <= *benchFiltersRounds && len(after) > 0; round++ {
		passes = append(passes, timedPass{*benchFiltersRounds + round, after})
	}
	for _, pass := range passes {
		round := pass.round
		timedRound := filterRound{Round: round, Start: time.Now().Format("2006-01-02 15:04:05")}
		for _, s := range pass.searchers {
			openOnly(s)
			s.prepare()
			for _, f := range filters {
				curve := curves[key(s, f)]
				if curve == nil || curve.Points[0] == nil {
					continue
				}
				if !until.IsZero() && time.Now().After(until) {
					t.Logf("round %d, %s %s, %s: not timed, it is past %s", round, s.index, s.path, f.Name, *benchFiltersUntil)
					timedRound.Skipped = append(timedRound.Skipped, s.index+" "+s.path+", "+f.Name)
					continue
				}
				point := curve.Points[0]
				openOnly(s)
				s.prepare()
				s.setEF(point.EF)
				timed := filterTimed{Index: s.index, Path: s.path, Filter: f.Name, Target: filterTargets[0], EF: point.EF}
				if *benchFiltersMemory {
					timed.Memory = &filterMemory{BeforeUntimed: sampleMemory()}
				}
				var profile *os.File
				if *benchFiltersUntimedProfiles != "" {
					var err error
					profile, err = os.Create(filepath.Join(*benchFiltersUntimedProfiles, fmt.Sprintf("round-%d-%s-%s-%s.pprof",
						round, s.index, s.path, strings.NewReplacer(" ", "", "%", "pct").Replace(f.Name))))
					require.NoError(t, err)
					require.NoError(t, pprof.StartCPUProfile(profile))
				}
				_, _, hits, _ := run(s, f, all)
				if profile != nil {
					pprof.StopCPUProfile()
					require.NoError(t, profile.Close())
				}
				timed.Recall = float64(hits) / float64(len(queries)*k)
				if s.index == "ours" {
					require.Equal(t, point.Recall, timed.Recall, "%s %s, %s: the recall the point was found with", s.index,
						s.path, f.Name)
				} else {
					require.InDelta(t, point.Recall, timed.Recall, 1e-3, "%s %s, %s: the recall the point was found with",
						s.index, s.path, f.Name)
				}
				if timed.Memory != nil {
					timed.Memory.BeforeWarm = sampleMemory()
				}
				for pass := 0; pass < *benchDiskPasses; pass++ {
					latencies, stats, _, wall := run(s, f, all)
					timed.Warm = append(timed.Warm, diskLatenciesOf(latencies, stats, wall))
					timed.Reads = mean(stats, func(s diskSearchStats) int { return s.Rescored })
					timed.Loads = mean(stats, func(s diskSearchStats) int { return s.Postings })
				}
				if timed.Memory != nil {
					timed.Memory.AfterWarm = sampleMemory()
				}
				warm := timed.Warm[len(timed.Warm)/2]
				t.Logf("round %d, %s %s, %s (ef %d), 1 thread, warm: p50 %.3f ms, p95 %.3f, p99 %.3f | %.0f QPS | %.1f loads, "+
					"%.1f vectors read", round, s.index, s.path, f.Name, point.EF, warm.P50, warm.P95, warm.P99, warm.QPS,
					timed.Loads, timed.Reads)
				if m := timed.Memory; m != nil {
					t.Logf("round %d, %s %s, %s, the untimed pass: the memory of the system, untimed: %s", round, s.index,
						s.path, f.Name, memoryChange(m.BeforeUntimed, m.BeforeWarm))
					t.Logf("round %d, %s %s, %s, the warm passes: the memory of the system, untimed: %s", round, s.index,
						s.path, f.Name, memoryChange(m.BeforeWarm, m.AfterWarm))
				}

				if *benchDiskDrop != "" {
					const batch = 10
					var latencies []time.Duration
					var stats []diskSearchStats
					var halves [2]struct {
						latencies []time.Duration
						stats     []diskSearchStats
						wall      time.Duration
					}
					var wall, reopen time.Duration
					coldHits := 0
					if timed.Memory != nil {
						before := sampleMemory()
						timed.Memory.BeforeCold = &before
					}
					for from := 0; from < *benchDiskCold; from += batch {
						which := all[from:min(from+batch, *benchDiskCold)]
						closeAll()
						out, err := exec.CommandContext(ctx, "sh", "-c", *benchDiskDrop).CombinedOutput()
						require.NoErrorf(t, err, "drop the cache: %s", out)
						reopen += openOnly(s)
						s.prepare()
						s.setEF(point.EF)
						batchLatencies, batchStats, batchHits, batchWall := run(s, f, which)
						half := &halves[timed.ColdBatches%2]
						half.latencies = append(half.latencies, batchLatencies...)
						half.stats = append(half.stats, batchStats...)
						half.wall += batchWall
						latencies, stats = append(latencies, batchLatencies...), append(stats, batchStats...)
						wall += batchWall
						coldHits += batchHits
						timed.ColdBatches++
					}
					if timed.Memory != nil {
						after := sampleMemory()
						timed.Memory.AfterCold = &after
					}
					cold := diskLatenciesOf(latencies, stats, wall)
					timed.Cold, timed.ColdBatch = &cold, batch
					for _, half := range halves {
						timed.ColdHalves = append(timed.ColdHalves, diskLatenciesOf(half.latencies, half.stats, half.wall))
					}
					timed.ColdRecall = float64(coldHits) / float64(len(latencies)*k)
					timed.ReopenMs = ms(reopen) / float64(timed.ColdBatches)
					t.Logf("round %d, %s %s, %s (ef %d), 1 thread, cold: p50 %.3f ms, p95 %.3f, p99 %.3f | %d batches of %d, "+
						"halves at p50 %.3f and %.3f ms | %.1f loads, %.1f vectors read", round, s.index, s.path, f.Name, point.EF,
						cold.P50, cold.P95, cold.P99, timed.ColdBatches, batch, timed.ColdHalves[0].P50, timed.ColdHalves[1].P50,
						mean(stats, func(s diskSearchStats) int { return s.Postings }),
						mean(stats, func(s diskSearchStats) int { return s.Rescored }))
					if m := timed.Memory; m != nil {
						t.Logf("round %d, %s %s, %s, the cold batches: the memory of the system, untimed: %s", round, s.index,
							s.path, f.Name, memoryChange(*m.BeforeCold, *m.AfterCold))
					}
				}
				timedRound.Points = append(timedRound.Points, timed)
				result.Rounds = slices.DeleteFunc(result.Rounds, func(r filterRound) bool { return r.Round == round })
				result.Rounds = append(result.Rounds, timedRound)
				write()
				if probeSearcher != nil {
					if onDisk.opened == nil || onDisk.opened.index != probeSearcher.index {
						openOnly(probeSearcher)
					}
					probe(fmt.Sprintf("after round %d, %s %s, %s", round, s.index, s.path, f.Name))
				}
			}
		}
		timedRound.End = time.Now().Format("2006-01-02 15:04:05")
		result.Rounds = slices.DeleteFunc(result.Rounds, func(r filterRound) bool { return r.Round == round })
		result.Rounds = append(result.Rounds, timedRound)
		write()
	}
	openOnly(searchers[0])
	if probeSearcher != nil {
		openOnly(probeSearcher)
		probe("at the end")
	}
}

// filteredIndex is the integrated index the measure opens: the store and the
// index over it, with nothing of what the index does by itself.
type filteredIndex struct {
	t      *testing.T
	ctx    context.Context
	logger logrus.FieldLogger
	dir    string
	store  *lsmkv.Store
	p      *Persistent
	uc     ent.UserConfig
	cutoff int
	closed bool
}

func openFilteredIndex(t *testing.T, ctx context.Context, logger logrus.FieldLogger, dir string) *filteredIndex {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "%s holds no index", dir)
	ix := &filteredIndex{t: t, ctx: ctx, logger: logger, dir: dir, uc: ent.NewDefaultUserConfig(), closed: true}
	ix.open()
	require.NotNil(t, ix.p.g, "the index in %s has no base", dir)
	return ix
}

func (ix *filteredIndex) open() {
	if !ix.closed {
		return
	}
	t := ix.t
	metrics, err := diskStoreMetrics()
	require.NoError(t, err)
	noop := cyclemanager.NewCallbackGroupNoop()
	ix.store, err = lsmkv.New(filepath.Join(ix.dir, "lsm"), ix.dir, ix.logger, metrics, nil, noop, noop, noop)
	require.NoError(t, err)
	makeOptions := func(strategy string, custom ...lsmkv.BucketOption) []lsmkv.BucketOption {
		return lsmkv.MakeRegularBucketOptions(strategy, append(custom, lsmkv.WithPread(true))...)
	}
	cfg := PersistentConfig{
		ID:                "main",
		ShardName:         "integration",
		ClassName:         "Integration",
		RootPath:          ix.dir,
		Logger:            ix.logger,
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: makeOptions,
		Seed:              1,
		noMaintenance:     true,
		flatCutoff:        ix.cutoff,
	}
	ix.p, err = Open(cfg, ix.uc, ix.store)
	require.NoError(t, err)
	ix.closed = false
}

func (ix *filteredIndex) shut() {
	if ix.closed {
		return
	}
	require.NoError(ix.t, ix.p.Shutdown(ix.ctx))
	require.NoError(ix.t, ix.store.Shutdown(ix.ctx))
	ix.closed = true
}

// searcher is the index on one path: always flat, or never.
func (ix *filteredIndex) searcher(path string, cutoff int, queries [][]float32, k int) *filterSearcher {
	return &filterSearcher{
		index: "ours", path: path,
		prepare: func() {
			ix.cutoff = cutoff
			ix.p.cfg.flatCutoff = cutoff
		},
		setEF: func(ef int) {
			ix.uc.EF = ef
			require.NoError(ix.t, ix.p.UpdateUserConfig(ix.uc, func() {}))
		},
		search: func(ctx context.Context, q int, allow helpers.AllowList) ([]uint64, diskSearchStats, error) {
			ix.p.mu.RLock()
			defer ix.p.mu.RUnlock()
			var stats diskSearchStats
			ids, _, err := ix.p.searchIndex(ctx, queries[q], k, allow, &stats)
			return ids, stats, err
		},
		close: ix.shut,
		open:  ix.open,
	}
}

// filterHNSW is HNSW as the measure builds it: the defaults of a class with
// 1-bit RQ, the graph and the codes in memory, and its vectors in a file read
// with pread to rescore (TestHNSWDiskTimed of the experiments). The index is
// reached through closures: its type is not exported.
type filterHNSW struct {
	vectors      *hnswDiskVectors
	uc           enthnsw.UserConfig
	config       map[string]any
	buildSeconds float64
	searchFn     func(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, error)
	update       func(uc enthnsw.UserConfig) error
	shutdown     func(ctx context.Context) error
}

func (h *filterHNSW) shut(ctx context.Context) {
	_ = h.shutdown(ctx)
	_ = h.vectors.close()
}

func (h *filterHNSW) searcher(t *testing.T, strategy string, queries [][]float32, k int) *filterSearcher {
	return &filterSearcher{
		index: "hnsw", path: strategy,
		prepare: func() {
			h.uc.FilterStrategy = strategy
			require.NoError(t, h.update(h.uc))
		},
		setEF: func(ef int) {
			h.uc.EF = ef
			require.NoError(t, h.update(h.uc))
		},
		search: func(ctx context.Context, q int, allow helpers.AllowList) ([]uint64, diskSearchStats, error) {
			ids, err := h.searchFn(ctx, queries[q], k, allow)
			return ids, diskSearchStats{}, err
		},
		reads: h.vectors.reads.Load,
		close: func() { require.NoError(t, h.vectors.close()) },
		open:  func() { require.NoError(t, h.vectors.open()) },
	}
}

// buildFilterHNSW writes the train set, normalized, to a file of dir unless
// it is there, and builds HNSW over it in the production order: uncompressed
// up to the training limit, the upgrade, and the rest against the codes.
func buildFilterHNSW(t *testing.T, ctx context.Context, logger logrus.FieldLogger, data *benchData, dir string,
) *filterHNSW {
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, "vectors.f32")
	n := len(data.train)
	writeFilterVectors(t, data, path)
	vectors := newHNSWDiskVectors(path, n, data.dims)
	require.NoError(t, vectors.open())
	tempVectorForID := func(_ context.Context, id uint64, container *common.VectorSlice, _ common.BucketView,
	) ([]float32, error) {
		vec, err := vectors.read(id, container.Slice)
		if err != nil {
			return nil, err
		}
		container.Slice = vec
		return vec, nil
	}
	uc := enthnsw.UserConfig{}
	uc.SetDefaults()
	uc.RQ = enthnsw.RQConfig{
		Enabled: true, Bits: 1, Centering: enthnsw.DefaultRQCentering || *benchFiltersHNSWCentering,
		TrainingLimit: enthnsw.DefaultRQTrainingLimit,
		RescoreLimit:  enthnsw.DefaultBRQRescoreLimit,
	}
	uc.FilterStrategy = enthnsw.FilterStrategyAcorn
	h := &filterHNSW{vectors: vectors, uc: uc, config: map[string]any{
		"maxConnections": uc.MaxConnections, "efConstruction": uc.EFConstruction, "distance": uc.Distance,
		"rqBits": uc.RQ.Bits, "rqCentering": uc.RQ.Centering, "rqTrainingLimit": uc.RQ.TrainingLimit,
		"rqRescoreLimit": uc.RQ.RescoreLimit, "flatSearchCutoff": uc.FlatSearchCutoff, "filterStrategy": uc.FilterStrategy,
		"acornFilterRatio": config.DefaultHNSWAcornFilterRatio,
	}}
	store := testinghelpers.NewDummyStore(t)
	const id = "filters"
	index, err := hnsw.New(hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    id,
		Logger:                logger,
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			return vectors.read(id, nil)
		},
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		// What a node gives it from HNSW_ACORN_FILTER_RATIO: with none, HNSW
		// never runs ACORN (acornRan).
		AcornFilterRatio: config.DefaultHNSWAcornFilterRatio,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	index.PostStartup(ctx)
	h.searchFn = func(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, vector, k, allow)
		return ids, err
	}
	h.update = func(uc enthnsw.UserConfig) error {
		updated := make(chan struct{})
		if err := index.UpdateUserConfig(uc, func() { close(updated) }); err != nil {
			return err
		}
		<-updated
		return nil
	}
	h.shutdown = index.Shutdown

	begin := time.Now()
	insert := func(from, to int) {
		var next atomic.Int64
		next.Store(int64(from))
		workers := runtime.GOMAXPROCS(0)
		errs := make([]error, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= to {
						return
					}
					if err := index.Add(ctx, uint64(i), slices.Clone(data.train[i])); err != nil {
						errs[worker] = err
						return
					}
				}
			}, logger)
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
	}
	warm := min(uc.RQ.TrainingLimit, n)
	insert(0, warm)
	upgraded := make(chan struct{})
	require.NoError(t, index.Upgrade(func() { close(upgraded) }))
	<-upgraded
	require.True(t, index.Compressed(), "the index must be compressed after the upgrade")
	insert(warm, n)
	h.buildSeconds = time.Since(begin).Seconds()
	t.Logf("hnsw built: %d vectors in %s, %v", n, time.Since(begin).Round(time.Second), h.config)
	return h
}

// acornRan fails the measure where HNSW with acorn answered exactly as with
// sweeping, at every ef, on a filter that leaves fewer than the share of the
// ACORN ratio and no fewer than the cutoff of its flat search: ACORN did not
// run there. A harness that builds HNSW in its own process must give it the
// ratio a node does (hnsw.Config.AcornFilterRatio): with none, HNSW never
// runs ACORN and says nothing, and the first untimed run of this measure
// measured sweeping twice.
func acornRan(t *testing.T, filters []*benchFilter, curves map[string]*filterCurve,
	key func(*filterSearcher, *benchFilter) string,
) {
	t.Helper()
	ran, notRan := acornChecked(filters, curves, key)
	for _, name := range ran {
		t.Logf("hnsw acorn and sweeping differ on %s: ACORN ran", name)
	}
	for _, name := range notRan {
		t.Errorf("hnsw acorn answered as hnsw sweeping at every ef on %s: ACORN did not run", name)
	}
}

// acornChecked tells the filters where ACORN should run and did, and those
// where it should and did not, by the curves of HNSW with both strategies.
func acornChecked(filters []*benchFilter, curves map[string]*filterCurve,
	key func(*filterSearcher, *benchFilter) string,
) (ran, notRan []string) {
	acorn := &filterSearcher{index: "hnsw", path: enthnsw.FilterStrategyAcorn}
	sweeping := &filterSearcher{index: "hnsw", path: enthnsw.FilterStrategySweeping}
	for _, f := range filters {
		a, s := curves[key(acorn, f)], curves[key(sweeping, f)]
		if a == nil || s == nil || f.Share == 0 || f.Share > config.DefaultHNSWAcornFilterRatio ||
			a.Allowed < enthnsw.DefaultFlatSearchCutoff {
			continue
		}
		same := len(a.Grid) == len(s.Grid)
		for i := 0; same && i < len(a.Grid); i++ {
			same = a.Grid[i].Recall == s.Grid[i].Recall && a.Grid[i].VectorReads == s.Grid[i].VectorReads
		}
		if same {
			notRan = append(notRan, f.Name)
		} else {
			ran = append(ran, f.Name)
		}
	}
	return ran, notRan
}

// The guard of ACORN looks only where ACORN runs, a filter under the ratio
// and above the cutoff, and finds the curves of the first untimed run, where
// acorn answered as sweeping at every ef.
func TestAcornChecked(t *testing.T) {
	key := func(s *filterSearcher, f *benchFilter) string { return s.index + "/" + s.path + "/" + f.Name }
	grid := func(recalls ...float64) []filterCurvePoint {
		points := make([]filterCurvePoint, len(recalls))
		for i, r := range recalls {
			ef := 10 << i
			points[i] = filterCurvePoint{EF: ef, Recall: r, VectorReads: float64(ef)}
		}
		return points
	}
	tests := []struct {
		name            string
		share           float64
		allowed         int
		acorn, sweeping []filterCurvePoint
		wantRan         []string
		wantNotRan      []string
	}{
		{name: "the same at 10 %", share: 0.1, allowed: 99_000, acorn: grid(0.9, 0.95), sweeping: grid(0.9, 0.95), wantNotRan: []string{"f"}},
		{name: "different at 10 %", share: 0.1, allowed: 99_000, acorn: grid(0.92, 0.96), sweeping: grid(0.9, 0.95), wantRan: []string{"f"}},
		{name: "the same at 50 %, over the ratio", share: 0.5, allowed: 495_000, acorn: grid(0.9), sweeping: grid(0.9)},
		{name: "the same at 1 %, under the cutoff", share: 0.01, allowed: 9_900, acorn: grid(0.9), sweeping: grid(0.9)},
		{name: "no filter", share: 0, allowed: 990_000, acorn: grid(0.9), sweeping: grid(0.9)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &benchFilter{Name: "f", Share: tt.share}
			curves := map[string]*filterCurve{
				"hnsw/acorn/f":    {Allowed: tt.allowed, Grid: tt.acorn},
				"hnsw/sweeping/f": {Allowed: tt.allowed, Grid: tt.sweeping},
			}
			ran, notRan := acornChecked([]*benchFilter{f}, curves, key)
			require.Equal(t, tt.wantRan, ran)
			require.Equal(t, tt.wantNotRan, notRan)
		})
	}
}

// The points of a repeat are named as the drift detector marks them, and a
// name that is no searcher or no filter fails the measure before it starts.
// TestFilterSearchersClosedBeforeADrop pins B and C of the re-timing of
// 2026-11-25. They measured HNSW only, by -bench.filters.points; the
// integrated index, built and left out of the measure, stayed open on the
// volume under test, its unmount failed, and both processes failed at their
// first cold batch. Before every drop of the cache, whatever a searcher built
// holds open is closed, measured or not, and the one timed is the only one
// opened again.
func TestFilterSearchersClosedBeforeADrop(t *testing.T) {
	all := []string{"ours graph", "ours flat", "hnsw acorn", "hnsw sweeping"}
	tests := []struct {
		name    string
		hnsw    bool
		names   string
		points  []string
		want    []string
		wantErr string
	}{
		{name: "every searcher measured", hnsw: true, want: all},
		{name: "B and C: HNSW only, by the points", hnsw: true, points: []string{"hnsw acorn"}, want: []string{"hnsw acorn"}},
		{name: "HNSW only, by the searchers", hnsw: true, names: "hnsw acorn", want: []string{"hnsw acorn"}},
		{
			name: "ours only, with HNSW built", hnsw: true, names: "ours graph, ours flat",
			want: []string{"ours graph", "ours flat"},
		},
		{name: "A: ours only, with no HNSW", names: "ours graph,ours flat", want: []string{"ours graph", "ours flat"}},
		{name: "one path of ours: the other shares its files", hnsw: true, names: "ours flat", want: []string{"ours flat"}},
		{
			name: "the points and the searchers together", hnsw: true, names: "ours graph,hnsw acorn",
			points: []string{"ours graph"}, want: []string{"ours graph"},
		},
		{name: "no searcher named", hnsw: true, names: "hnsw flat", wantErr: `no searcher is named "hnsw flat"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// What lies open under each index, as the measure builds them: the
			// integrated index open since openFilteredIndex, and HNSW's file of
			// vectors open since buildFilterHNSW.
			open := map[string]bool{"ours": true}
			searcher := func(index, path string) *filterSearcher {
				return &filterSearcher{
					index: index, path: path,
					open:  func() { open[index] = true },
					close: func() { open[index] = false },
				}
			}
			built := []*filterSearcher{searcher("ours", "graph"), searcher("ours", "flat")}
			if tt.hnsw {
				open["hnsw"] = true
				built = append(built, searcher("hnsw", "acorn"), searcher("hnsw", "sweeping"))
			}
			pointed := func(s, _ string) bool { return tt.points == nil || slices.Contains(tt.points, s) }
			measured, onDisk, err := measuredFilterSearchers(built, tt.names, pointed)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			names := make([]string, len(measured))
			for i, s := range measured {
				names[i] = s.index + " " + s.path
			}
			require.Equal(t, tt.want, names)
			openIndexes := func() []string {
				var on []string
				for index, isOpen := range open {
					if isOpen {
						on = append(on, index)
					}
				}
				return on
			}
			for _, s := range measured {
				onDisk.openOnly(s)
				require.Equal(t, []string{s.index}, openIndexes(), "%s %s timed", s.index, s.path)
				onDisk.closeAll()
				require.Empty(t, openIndexes(), "a drop of the cache with %s %s timed", s.index, s.path)
				onDisk.openOnly(s)
				require.Equal(t, []string{s.index}, openIndexes(), "%s %s timed, after the drop", s.index, s.path)
			}
		})
	}
}

func TestParseFilterPoints(t *testing.T) {
	filters := []string{"none", "random 10 %", "random 1 %", "near 10 %", "near 0.1 %", "far 0.1 %"}
	tests := []struct {
		name    string
		spec    string
		want    []filterPoint
		wantErr string
	}{
		{name: "empty: every point", spec: ""},
		{name: "blank: every point", spec: "  "},
		{
			name: "the five the detector marked on the night of the 23rd",
			spec: "ours graph:random 1 %,ours flat:random 10 %,ours flat:near 10 %,hnsw acorn:near 0.1 %,hnsw acorn:far 0.1 %",
			want: []filterPoint{
				{"ours graph", "random 1 %"},
				{"ours flat", "random 10 %"},
				{"ours flat", "near 10 %"},
				{"hnsw acorn", "near 0.1 %"},
				{"hnsw acorn", "far 0.1 %"},
			},
		},
		{
			name: "spaces around the names",
			spec: " hnsw sweeping : none , ours graph:none",
			want: []filterPoint{{"hnsw sweeping", "none"}, {"ours graph", "none"}},
		},
		{name: "no colon", spec: "ours graph random 1 %", wantErr: "separated by a colon"},
		{name: "no filter", spec: "ours graph:", wantErr: "separated by a colon"},
		{name: "no searcher", spec: ":random 1 %", wantErr: "separated by a colon"},
		{name: "an unknown searcher", spec: "ours scan:random 1 %", wantErr: `no searcher is named "ours scan"`},
		{name: "an unknown filter", spec: "ours graph:random 2 %", wantErr: `no filter is named "random 2 %"`},
		{name: "a filter left out by its kind", spec: "ours graph:far 10 %", wantErr: `no filter is named "far 10 %"`},
		{name: "a point twice", spec: "ours flat:near 10 %,ours flat:near 10 %", wantErr: "named twice"},
		{name: "a trailing comma", spec: "ours flat:near 10 %,", wantErr: "separated by a colon"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := parseFilterPoints(tt.spec, filterSearcherNames, filters)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, points)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, points)
		})
	}
}

// writeFilterVectors writes the train set, which is in memory and normalized,
// to path, unless a file of its size is there already.
func writeFilterVectors(t *testing.T, data *benchData, path string) {
	want := int64(len(data.train)) * int64(data.dims) * 4
	if info, err := os.Stat(path); err == nil && info.Size() == want {
		t.Logf("the vectors are in %s already", path)
		return
	}
	file, err := os.Create(path + ".tmp")
	require.NoError(t, err)
	w := bufio.NewWriterSize(file, 16<<20)
	buf := make([]byte, 4*data.dims)
	for _, vec := range data.train {
		for i, x := range vec {
			binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(x))
		}
		_, err := w.Write(buf)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	require.NoError(t, os.Rename(path+".tmp", path))
}

// hnswDiskVectors is the vectors of a dataset in a file of float32, read one
// at a time with pread, as HNSW reads the vector of a node to rescore it
// (TestHNSWDiskTimed of the experiments). close and open let the cache of the
// file be dropped between two batches of queries.
type hnswDiskVectors struct {
	path  string
	dims  int
	rows  uint64
	file  atomic.Pointer[os.File]
	reads atomic.Int64
	bufs  sync.Pool
}

func newHNSWDiskVectors(path string, rows, dims int) *hnswDiskVectors {
	v := &hnswDiskVectors{path: path, dims: dims, rows: uint64(rows)}
	v.bufs.New = func() any {
		buf := make([]byte, 4*dims)
		return &buf
	}
	return v
}

func (v *hnswDiskVectors) open() error {
	f, err := os.Open(v.path)
	if err != nil {
		return err
	}
	v.file.Store(f)
	return nil
}

func (v *hnswDiskVectors) close() error {
	if f := v.file.Swap(nil); f != nil {
		return f.Close()
	}
	return nil
}

// read reads the vector of id into into, which it grows as it needs.
func (v *hnswDiskVectors) read(id uint64, into []float32) ([]float32, error) {
	if id >= v.rows {
		return nil, storobj.NewErrNotFoundf(id, "out of range")
	}
	f := v.file.Load()
	if f == nil {
		return nil, fmt.Errorf("read vector %d: the file of the vectors is closed", id)
	}
	buf := v.bufs.Get().(*[]byte)
	defer v.bufs.Put(buf)
	if _, err := f.ReadAt(*buf, int64(id)*int64(len(*buf))); err != nil {
		return nil, fmt.Errorf("read vector %d: %w", id, err)
	}
	v.reads.Add(1)
	into = slices.Grow(into[:0], v.dims)[:v.dims]
	for i := range into {
		into[i] = math.Float32frombits(binary.LittleEndian.Uint32((*buf)[4*i:]))
	}
	return into, nil
}
