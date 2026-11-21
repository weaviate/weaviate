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
	"context"
	"encoding/json"
	"flag"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// The integrated index at scale (integration-plan.md, section 6): the index a
// shard opens, Persistent, grown from empty to the whole train set through
// its own AddBatch, with the defaults of a class, and then with a share of it
// deleted. It counts and times nothing that is to be compared: recall at
// equal ef, loads a query and coverage, next to the index v5.0 grew
// (testdata/bench/v5.0/dbpedia-1m-grow-manifest.json) and to the cell 1b of
// v6.0. The vectors go in in the order of v5.0, so that with a base of the
// first tenth the two runs differ in what a run cannot repeat, the levels the
// centroid graph draws, and in the order the base is built from.
//
// The index is opened over a store of its own that stands for the one of a
// shard, with its flush and compaction cycles called every growCycleEvery
// inserts, as v5.0 calls those of its store.
//
// Once grown and measured, and again once the share is deleted and measured,
// the index and its store are closed and opened from what they left on disk,
// as a node that restarts does, and measured again: what was held and what
// was deleted must be the same, and so must every answer. A search is asked
// twice in every stage, so that an answer that changes between two askings of
// the same open index is not taken for one the reopening changed.
var (
	benchIntegrationDir = flag.String("bench.integrationdir", "", "the integrated index at scale "+
		"(TestIntegrationScale): the directory the index and its store are written to, which must be empty")
	benchIntegrationBase = flag.Int("bench.integrationbase", 0, "with TestIntegrationScale: the vectors the index "+
		"gathers before it builds its first postings, 0 for its default")
	benchIntegrationOut = flag.String("bench.integrationout", "", "with TestIntegrationScale: the JSON file its "+
		"result is written to, after every stage")
	benchIntegrationDelete = flag.Int("bench.integrationdelete", 30, "with TestIntegrationScale: the share of the "+
		"vectors, in percent, deleted at random once the index is grown and measured, 0 for none")
	benchIntegrationEFs = flag.String("bench.integrationefs", "21,158,20,40,80,160", "with TestIntegrationScale: the "+
		"ef the index is measured at, separated by commas: by default the two operating points of v5.0 and the "+
		"grid of v6.0 around them")
	benchIntegrationUntil = flag.String("bench.integrationuntil", "", "with TestIntegrationScale: the time of day, "+
		"HH:MM, past which the run stops inserting, closes the index and fails, so that nothing of it is alive later")
	benchIntegrationReopen = flag.Bool("bench.integrationreopen", true, "with TestIntegrationScale: after each "+
		"stage the index and its store are closed, opened again from disk and measured again")
	benchIntegrationReads = flag.Bool("bench.integrationreads", true, "with TestIntegrationScale and "+
		"-bench.integrationreopen: once opened again the store reads with pread and counts what it reads, a load of "+
		"a posting is told in reads and bytes of the store, and the index is measured once more with its store "+
		"compacted until it finds nothing to compact and its vectors laid out again")
)

type integrationPoint struct {
	EF     int     `json:"ef"`
	Recall float64 `json:"recall"`
	Loads  float64 `json:"loads"`
	Rounds float64 `json:"rounds"`
	// AskedTwice counts the queries whose answer, asked a second time in the
	// same stage, was another one.
	AskedTwice int `json:"queriesAnsweredOtherwiseTheSecondTime"`
	// Reopened, in a stage that is compared with the one before it, counts the
	// queries whose answer is not the one they had there: over the index
	// closed and opened again, or compacted and laid out again.
	Reopened *int `json:"queriesAnsweredOtherwiseAfterReopening,omitempty"`
	// StoreReads and StoreBytes are what the store read for a load of a
	// posting, its two rows, where the store counts.
	StoreReads float64 `json:"storeReadsPerLoad,omitempty"`
	StoreBytes float64 `json:"storeBytesPerLoad,omitempty"`
}

type integrationStage struct {
	Name     string `json:"name"`
	Held     int    `json:"held"`
	Deleted  int    `json:"deleted"`
	Postings int    `json:"postingsGivenOut"`
	Splits   int    `json:"splits"`
	Coverage string `json:"coverage"`
	// ReopenSeconds, in a stage over the index closed and opened again, is
	// how long opening its store and the index took. It is not a timing: the
	// machine is not alone.
	ReopenSeconds float64 `json:"reopenSeconds,omitempty"`
	// Segments is how many segments the bucket of postings has on disk.
	// VectorGen, Slots and VectorsLaidOut tell of the file of vectors: its
	// generation, the vectors it holds and how many of them are in the order
	// of their postings. Layouts counts the passes over the vectors that
	// ended since the index was opened.
	Segments       int                `json:"postingSegments"`
	VectorGen      int                `json:"vectorGen,omitempty"`
	Slots          int                `json:"vectorSlots,omitempty"`
	VectorsLaidOut int                `json:"vectorsLaidOut,omitempty"`
	Layouts        int64              `json:"layoutsSinceOpened"`
	Points         []integrationPoint `json:"points"`
}

type integrationStretch struct {
	Inserted  int     `json:"inserted"`
	Seconds   float64 `json:"seconds"`
	PerSecond float64 `json:"perSecond"`
	Postings  int     `json:"postingsGivenOut"`
}

type integrationResult struct {
	Dataset      string               `json:"dataset"`
	Vectors      int                  `json:"vectors"`
	Queries      int                  `json:"queries"`
	K            int                  `json:"k"`
	Base         int                  `json:"base"`
	UserConfig   ent.UserConfig       `json:"userConfig"`
	Meta         growMeta             `json:"meta"`
	Procs        int                  `json:"gomaxprocs"`
	BaseSeconds  float64              `json:"baseSeconds"`
	InsertsAfter int                  `json:"insertsAfterTheBase"`
	Seconds      float64              `json:"insertSeconds"`
	PerSecond    float64              `json:"insertsPerSecond"`
	Stretches    []integrationStretch `json:"stretches"`
	Stats        growStats            `json:"stats"`
	DirBytes     int64                `json:"dirBytes"`
	Stages       []integrationStage   `json:"stages"`
}

func TestIntegrationScale(t *testing.T) {
	if *benchDatasetDir == "" || *benchIntegrationDir == "" || *benchIntegrationOut == "" {
		t.Skip("no -bench.dataset, -bench.integrationdir and -bench.integrationout given")
	}
	ctx := context.Background()
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, false)
	if *benchLimit > 0 {
		data.neighbors = bruteNeighbors(data, streamTruthDepth)
	}
	require.NotNil(t, data.neighbors, "the ground truth is drawn from the shipped one")
	n, k := len(data.train), *benchK
	// The order of v5.0: its base is the first tenth of it.
	order := make([]uint64, n)
	for i := range order {
		order[i] = uint64(i)
	}
	rand.New(rand.NewPCG(1, 1)).Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })

	var until time.Time
	if *benchIntegrationUntil != "" {
		clock, err := time.ParseInLocation("15:04", *benchIntegrationUntil, time.Local)
		require.NoError(t, err, "-bench.integrationuntil")
		now := time.Now()
		until = time.Date(now.Year(), now.Month(), now.Day(), clock.Hour(), clock.Minute(), 0, 0, time.Local)
		require.True(t, until.After(now), "-bench.integrationuntil %s is past already", *benchIntegrationUntil)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	dir := *benchIntegrationDir
	require.NoError(t, os.MkdirAll(dir, 0o755))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "%s is not empty", dir)

	var (
		flush, compaction cyclemanager.CycleCallbackGroup
		store             *lsmkv.Store
		p                 *Persistent
	)
	uc := ent.NewDefaultUserConfig()
	closed := true
	// The store has its metrics from the first one that opens: it keeps one
	// observer of its reads for the whole process. It counts only what it
	// reads with pread, which is how it reads once counting is set.
	metrics, err := diskStoreMetrics()
	require.NoError(t, err)
	counting := false
	// open opens the store and the index over it: empty the first time, and
	// after a shut what the ones before left on disk.
	open := func() {
		flush = cyclemanager.NewCallbackGroup("flush", logger, 1)
		compaction = cyclemanager.NewCallbackGroup("compaction", logger, 1)
		var err error
		store, err = lsmkv.New(filepath.Join(dir, "lsm"), dir, logger, metrics, nil, compaction,
			cyclemanager.NewCallbackGroupNoop(), flush)
		require.NoError(t, err)
		makeOptions := lsmkv.MakeBucketOptions(lsmkv.MakeRegularBucketOptions)
		if counting {
			makeOptions = func(strategy string, custom ...lsmkv.BucketOption) []lsmkv.BucketOption {
				return lsmkv.MakeRegularBucketOptions(strategy, append(custom, lsmkv.WithPread(true))...)
			}
		}
		p, err = Open(PersistentConfig{
			ID:                "main",
			ShardName:         "integration",
			ClassName:         "Integration",
			RootPath:          dir,
			Logger:            logger,
			DistanceProvider:  distancer.NewCosineDistanceProvider(),
			MakeBucketOptions: makeOptions,
			BaseSize:          *benchIntegrationBase,
			Seed:              1,
		}, uc, store)
		require.NoError(t, err)
		closed = false
	}
	// segments counts the segments of the bucket of postings on disk.
	segments := func() int {
		found, err := filepath.Glob(filepath.Join(dir, "lsm",
			helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketPostings, "main"), "segment-*.db"))
		require.NoError(t, err)
		return len(found)
	}
	cycle := func() {
		never := func() bool { return false }
		flush.CycleCallback(never)
		compaction.CycleCallback(never)
	}
	shut := func() {
		if closed {
			return
		}
		closed = true
		require.NoError(t, p.Flush())
		require.NoError(t, p.Shutdown(ctx))
		require.NoError(t, store.Shutdown(ctx))
	}
	open()
	defer shut()

	result := &integrationResult{
		Dataset: data.name, Vectors: n, Queries: len(data.queries), K: k, Base: p.cfg.BaseSize, UserConfig: uc,
		Procs: runtime.GOMAXPROCS(0),
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchIntegrationOut, raw, 0o644))
	}

	// The inserts, through the index a shard opens: batches of a request,
	// flushed as a shard flushes its indexes after a request that writes.
	const batch = 100
	inserted, afterBase := 0, 0
	begin := time.Now()
	var insertTime time.Duration
	lastLog, lastInserted := time.Now(), 0
	vectors := make([][]float32, 0, batch)
	for inserted < n {
		if !until.IsZero() && time.Now().After(until) {
			shut()
			t.Fatalf("STOP: it is past %s with %d of %d vectors in: the run ends here, with nothing of it alive",
				*benchIntegrationUntil, inserted, n)
		}
		to := min(inserted+batch, n)
		vectors = vectors[:0]
		for _, id := range order[inserted:to] {
			vectors = append(vectors, data.train[id])
		}
		wasBuilt := p.g != nil
		clock := time.Now()
		if err := p.AddBatch(ctx, order[inserted:to], vectors); err != nil {
			t.Fatalf("inserts %d to %d: %v", inserted, to, err)
		}
		require.NoError(t, p.Flush())
		took := time.Since(clock)
		switch {
		case !wasBuilt && p.g != nil:
			// The batch that completed the base built it.
			result.BaseSeconds = took.Seconds()
			result.Base = p.g.meta.Live
			t.Logf("the base: %d vectors in %d postings, built in %s inside the insert that completed it", p.g.meta.Live,
				p.g.meta.Postings, took.Round(time.Second))
			lastLog, lastInserted = time.Now(), to
		case wasBuilt:
			insertTime += took
			afterBase += to - inserted
		}
		inserted = to
		if inserted%growCycleEvery == 0 {
			cycle()
		}
		if p.g != nil && (time.Since(lastLog) > 5*time.Minute || inserted == n) {
			stretch := integrationStretch{
				Inserted: inserted, Seconds: time.Since(begin).Seconds(), Postings: p.g.meta.Postings,
				PerSecond: float64(inserted-lastInserted) / time.Since(lastLog).Seconds(),
			}
			result.Stretches = append(result.Stretches, stretch)
			result.InsertsAfter, result.Seconds = afterBase, insertTime.Seconds()
			if insertTime > 0 {
				result.PerSecond = float64(afterBase) / insertTime.Seconds()
			}
			// What the index does in the background writes its meta too.
			p.mu.RLock()
			result.Meta, result.Stats = p.g.meta, p.g.stats
			p.mu.RUnlock()
			write()
			t.Logf("%d of %d in (%.1f %%): %.0f inserts a second in this stretch, %.0f since the base | %d postings "+
				"given out, %d splits | %d passes over the vectors, %d of %d in the order of their postings", inserted, n,
				100*float64(inserted)/float64(n), stretch.PerSecond, result.PerSecond, result.Meta.Postings,
				result.Meta.Splits, p.layouts.Load(), result.Meta.VectorsLaidOut, result.Meta.Live)
			lastLog, lastInserted = time.Now(), inserted
		}
	}
	require.NotNil(t, p.g, "the train set is smaller than the base: nothing was built")
	g := p.g
	cycle()

	var efs []int
	for _, field := range strings.Split(*benchIntegrationEFs, ",") {
		ef, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err, "-bench.integrationefs")
		efs = append(efs, ef)
	}
	// reopened tells that the index is not the one that took the inserts:
	// what it counts is no longer what they did.
	reopened := false
	// reopen closes the index and its store and opens them again from disk,
	// and returns how long opening them took. What the index holds must be
	// what it held.
	reopen := func(name string) time.Duration {
		// What it does in the background ends first: it writes the meta too.
		p.stopMaintenance()
		meta, deletedCount := g.meta, g.deletedCount
		shut()
		counting = *benchIntegrationReads
		clock := time.Now()
		open()
		took := time.Since(clock)
		reopened = true
		require.NotNil(t, p.g, "STOP: %s: the index opened again has no base", name)
		g = p.g
		p.mu.RLock()
		assert.Equal(t, meta, g.meta, "%s: the meta of the index opened again", name)
		assert.Equal(t, deletedCount, g.deletedCount, "%s: the deleted vectors of the index opened again", name)
		p.mu.RUnlock()
		t.Logf("%s: the store and the index closed and opened again in %s, with other runs on the machine: %d vectors, "+
			"%d of them deleted, %d postings given out", name, took.Round(time.Millisecond), g.meta.Live, g.deletedCount,
			g.meta.Postings)
		return took
	}
	// measure takes the recall of the index at every ef, through the search
	// of a shard, and what a query loads, from the search under it, and
	// returns the answers at every ef. No answer may hold a vector that live
	// leaves out: nil is all of them. With the answers of the stage before,
	// it counts the queries the index answers otherwise than it did there,
	// which was before what since tells.
	measure := func(name string, truth [][]uint64, live []bool, deleted int, before [][][]uint64, since string,
		reopenTook time.Duration,
	) [][][]uint64 {
		answers := make([][][]uint64, 0, len(efs))
		// Nothing of the index runs next to the check of its coverage, not
		// what it does in the background either.
		p.writeMu.Lock()
		p.mu.Lock()
		stage := integrationStage{
			Name: name, Held: g.meta.Live - g.deletedCount, Deleted: deleted, Postings: g.meta.Postings,
			Splits: g.meta.Splits, Coverage: "complete", ReopenSeconds: reopenTook.Seconds(),
			Segments: segments(), VectorGen: g.meta.VectorGen, Slots: int(g.nextSlot),
			VectorsLaidOut: g.meta.VectorsLaidOut, Layouts: p.layouts.Load(),
		}
		cleaned := g.cleanCentroidGraph()
		if err := g.checkCoverage(); err != nil {
			stage.Coverage = err.Error()
		}
		p.mu.Unlock()
		p.writeMu.Unlock()
		require.NoError(t, cleaned)
		t.Logf("%s: coverage %s | %d segments of postings, generation %d of the vectors, %d of %d in the order of "+
			"their postings", name, stage.Coverage, stage.Segments, stage.VectorGen, stage.VectorsLaidOut, stage.Slots)
		for at, ef := range efs {
			searchUC := uc
			searchUC.EF = ef
			require.NoError(t, p.UpdateUserConfig(searchUC, func() {}))
			var hits, loads, rounds, next, returned, askedTwice atomic.Int64
			answered := make([][]uint64, len(data.queries))
			readsBefore, bytesBefore, err := diskStoreReads()
			require.NoError(t, err)
			eg := enterrors.NewErrorGroupWrapper(logger)
			for range 8 {
				eg.Go(func() error {
					for {
						q := int(next.Add(1)) - 1
						if q >= len(data.queries) {
							return nil
						}
						ids, _, err := p.SearchByVector(ctx, data.queries[q], k, nil)
						if err != nil {
							return err
						}
						found := 0
						for _, want := range truth[q] {
							if slices.Contains(ids, want) {
								found++
							}
						}
						hits.Add(int64(found))
						for _, id := range ids {
							if live != nil && !live[id] {
								returned.Add(1)
							}
						}
						answered[q] = ids
						stats := &diskSearchStats{}
						// As the search of a shard holds the index.
						p.mu.RLock()
						again, _, err := g.search(ctx, data.queries[q], k, stats)
						p.mu.RUnlock()
						if err != nil {
							return err
						}
						if !slices.Equal(ids, again) {
							askedTwice.Add(1)
						}
						loads.Add(int64(stats.Postings))
						rounds.Add(int64(stats.Rounds))
					}
				})
			}
			require.NoError(t, eg.Wait())
			require.Zero(t, returned.Load(), "STOP: %s, ef %d: deleted vectors were returned", name, ef)
			queries := float64(len(data.queries))
			point := integrationPoint{
				EF: ef, Recall: float64(hits.Load()) / (queries * float64(k)),
				Loads: float64(loads.Load()) / queries, Rounds: float64(rounds.Load()) / queries,
				AskedTwice: int(askedTwice.Load()),
			}
			t.Logf("%s, ef %d: recall %.4f, %.2f loads a query in %.2f rounds, %d of %d queries answered otherwise the "+
				"second time", name, ef, point.Recall, point.Loads, point.Rounds, point.AskedTwice, len(data.queries))
			if counting && loads.Load() > 0 {
				// Every query was searched twice, and loaded the same both
				// times: the store read for twice the loads counted.
				readsAfter, bytesAfter, err := diskStoreReads()
				require.NoError(t, err)
				searched := 2 * float64(loads.Load())
				point.StoreReads = float64(readsAfter-readsBefore) / searched
				point.StoreBytes = float64(bytesAfter-bytesBefore) / searched
				t.Logf("%s, ef %d: the store read %.2f times and %.2f KB for a load", name, ef, point.StoreReads,
					point.StoreBytes/1024)
			}
			if before != nil {
				changed := 0
				for q := range answered {
					if !slices.Equal(answered[q], before[at][q]) {
						changed++
					}
				}
				point.Reopened = &changed
				t.Logf("%s, ef %d: %d of %d queries answered otherwise than before %s", name, ef, changed,
					len(data.queries), since)
			}
			stage.Points = append(stage.Points, point)
			answers = append(answers, answered)
		}
		require.NoError(t, p.UpdateUserConfig(uc, func() {}))
		result.Stages = append(result.Stages, stage)
		p.mu.RLock()
		result.Meta = g.meta
		if !reopened {
			result.Stats = g.stats
		}
		p.mu.RUnlock()
		write()
		require.Equal(t, "complete", stage.Coverage, "STOP: the coverage of the index, %s", name)
		return answers
	}
	require.NoError(t, p.Flush())
	result.DirBytes, err = sizeOnDisk(dir)
	require.NoError(t, err)
	// The directory is not named: the log is kept next to the result.
	t.Logf("grown: %d vectors, %d postings given out, %d splits, %.1f GB in its directory", g.meta.Live,
		g.meta.Postings, g.meta.Splits, float64(result.DirBytes)/1e9)
	truth := liveTruth(t, data, nil, k)
	grown := measure("grown", truth, nil, 0, nil, "", 0)
	if *benchIntegrationReopen {
		took := reopen("grown")
		again := measure("grown, opened again", truth, nil, 0, grown, "the index was closed", took)
		if *benchIntegrationReads {
			// What a node that is left alone gets to: every memtable flushed,
			// the store compacted until it finds nothing more to compact,
			// and the vectors in the order of their postings.
			for name, bucket := range store.GetBucketsByName() {
				require.NoError(t, bucket.FlushAndSwitch(), "flush the bucket %s", name)
			}
			rounds := 0
			for compaction.CycleCallback(func() bool { return false }) {
				rounds++
			}
			passes := p.layouts.Load()
			p.mu.RLock()
			due := g.meta.VectorGen > 0 && g.meta.VectorsLaidOut < int(g.nextSlot)
			p.mu.RUnlock()
			if due {
				clock := time.Now()
				require.NoError(t, p.layOut(ctx))
				t.Logf("the vectors laid out again in %s, with other runs on the machine", time.Since(clock).Round(time.Second))
			}
			t.Logf("compacted in %d rounds, and %d passes over the vectors", rounds, p.layouts.Load()-passes)
			measure("grown, compacted and laid out", truth, nil, 0, again, "it was compacted and laid out", 0)
		}
	}

	if *benchIntegrationDelete > 0 {
		// A share of the vectors, at random, deleted as a shard deletes them.
		live := make([]bool, n)
		for i := range live {
			live[i] = true
		}
		victims := rand.New(rand.NewPCG(6, 0)).Perm(n)[:n**benchIntegrationDelete/100]
		begin := time.Now()
		for from := 0; from < len(victims); from += batch {
			ids := make([]uint64, 0, batch)
			for _, id := range victims[from:min(from+batch, len(victims))] {
				ids = append(ids, uint64(id))
				live[id] = false
			}
			require.NoError(t, p.Delete(ids...))
		}
		require.NoError(t, p.Flush())
		cycle()
		t.Logf("deleted %d vectors, %d %% of them, in %s", len(victims), *benchIntegrationDelete,
			time.Since(begin).Round(time.Second))
		for _, id := range victims[:1000] {
			require.False(t, p.ContainsDoc(uint64(id)), "doc %d was deleted", id)
		}
		truth, fallbacks := liveTruthOf(t, data, live, k)
		t.Logf("the ground truth of the vectors that are left: %d queries by brute force", fallbacks)
		after := measure("deleted", truth, live, len(victims), nil, "", 0)
		if *benchIntegrationReopen {
			took := reopen("deleted")
			// Every doc id, against what was deleted before the index closed.
			wrong := 0
			for id, held := range live {
				if p.ContainsDoc(uint64(id)) != held {
					wrong++
				}
			}
			assert.Zero(t, wrong, "deleted, opened again: doc ids held that were deleted, or deleted ones held")
			t.Logf("deleted, opened again: %d of %d doc ids are held or not against what was deleted", wrong, n)
			measure("deleted, opened again", truth, live, len(victims), after, "the index was closed", took)
		}
	}
}
