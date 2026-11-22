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
	benchIntegrationChurn = flag.Int("bench.integrationchurn", 0, "with TestIntegrationScale: the rounds of the churn "+
		"of v6.2, 0 for none. The index is its base alone, of -bench.integrationbase vectors built in batch, and "+
		"every round deletes a share of them at random, inserts as many new ones and runs a pass over the deleted "+
		"vectors. It is measured at the end, opened again, and next to an index whose base is a batch of the "+
		"vectors that are left")
	benchIntegrationChurnShare = flag.Float64("bench.integrationchurnshare", 0.03, "with -bench.integrationchurn: "+
		"the share of the base deleted and inserted in every round")
	benchIntegrationWriters = flag.Int("bench.integrationwriters", 1, "with TestIntegrationScale: how many callers "+
		"add to the index at once while it grows, a batch of a hundred vectors each (v5.2)")
	benchIntegrationCycles = flag.Bool("bench.integrationcycles", false, "with TestIntegrationScale: the index is "+
		"given the cycles of a node that keep an HNSW graph, for the graph over its centroids, called with those of "+
		"the store; a stage is then measured with the graph as they left it, and not cleaned by hand first")
	benchIntegrationCacheMode = flag.String("bench.integrationcachemode", ent.CacheModeRouting, "with "+
		"TestIntegrationScale: the rq.cacheMode of the class, routing or none. With none the index keeps no code in "+
		"memory, and once it has grown it is opened with them too, over the same files, to count the queries it then "+
		"answers otherwise")
	benchIntegrationReuse = flag.Bool("bench.integrationreuse", false, "with TestIntegrationScale: "+
		"-bench.integrationdir holds an index a run before left, which is opened as it is, with nothing put in: it "+
		"is measured as the cycles left it and again with its bucket of postings as one segment")
)

// integrationRound is a round of the churn: what was deleted and inserted,
// what the passes over the deleted vectors have done up to it, and what the
// index is left with.
type integrationRound struct {
	Round    int            `json:"round"`
	Deleted  int            `json:"deleted"`
	Inserted int            `json:"inserted"`
	Cleaned  growCleanStats `json:"cleanedSoFar"`
	// Held is the vectors the index holds after the pass, Postings its live
	// postings and EdgesPerVector the edges of a vector, on average.
	Held           int     `json:"held"`
	Postings       int     `json:"livePostings"`
	EdgesPerVector float64 `json:"edgesPerVector"`
	// PassSeconds is how long the pass took. It is not a timing: the
	// machine is not alone.
	PassSeconds float64 `json:"passSeconds"`
}

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
	Segments int `json:"postingSegments"`
	// DeletedCentroids is how many centroids of postings that were split or
	// merged the graph still held, deleted, when the stage started.
	DeletedCentroids int `json:"deletedCentroidsInTheGraph"`
	// AsOneSegment counts the times the index asked for its bucket of
	// postings as one segment and got it, since it was opened: each of them
	// had the store write the bucket again, whole.
	AsOneSegment int `json:"postingBucketAsOneSegment"`
	// CodesInMemory is the bytes of the routing codes the index holds in
	// memory: none with rq.cacheMode none.
	CodesInMemory  int64              `json:"routingCodesInMemoryBytes"`
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
	// Refreshed is what the refreshes of edges did while the index grew.
	Refreshed growRefreshStats `json:"refreshed"`
	// JournalRecords and JournalBytes are what the index appended to its
	// journal while it grew: a record for every write of it. Recovered is
	// what an index that a run before left found when it was opened: the
	// records of its journal it applied again, and whether it built its
	// centroid graph again.
	JournalRecords int64          `json:"journalRecords"`
	JournalBytes   int64          `json:"journalBytes"`
	Recovered      *growRecovered `json:"recovered,omitempty"`
	// Reference, after a churn, is the meta of the index built in batch from
	// the vectors that were left.
	Reference *growMeta          `json:"referenceMeta,omitempty"`
	Rounds    []integrationRound `json:"rounds,omitempty"`
	Stages    []integrationStage `json:"stages"`
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
	reuse := *benchIntegrationReuse
	if reuse {
		require.NotEmpty(t, entries, "%s holds no index to open", dir)
	} else {
		require.Empty(t, entries, "%s is not empty", dir)
	}

	var (
		flush, compaction      cyclemanager.CycleCallbackGroup
		tombstones, commitLogs cyclemanager.CycleCallbackGroup
		store                  *lsmkv.Store
		p                      *Persistent
	)
	uc := ent.NewDefaultUserConfig()
	uc.RQ.CacheMode = *benchIntegrationCacheMode
	closed := true
	// The store has its metrics from the first one that opens: it keeps one
	// observer of its reads for the whole process. It counts only what it
	// reads with pread, which is how it reads once counting is set.
	metrics, err := diskStoreMetrics()
	require.NoError(t, err)
	// An index that a run before left is read with pread from the start.
	counting := reuse && *benchIntegrationReads
	// baseSize is the vectors the index gathers before it builds its base.
	// In the churn nothing runs in the background: the passes over the
	// deleted vectors are called, one a round.
	baseSize, churn := *benchIntegrationBase, *benchIntegrationChurn
	if churn > 0 {
		require.Positive(t, baseSize, "-bench.integrationchurn takes the base of -bench.integrationbase")
		require.Less(t, baseSize, n)
	}
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
		cfg := PersistentConfig{
			ID:                "main",
			ShardName:         "integration",
			ClassName:         "Integration",
			RootPath:          dir,
			Logger:            logger,
			DistanceProvider:  distancer.NewCosineDistanceProvider(),
			MakeBucketOptions: makeOptions,
			BaseSize:          baseSize,
			Seed:              1,
			noMaintenance:     churn > 0 || reuse,
		}
		if *benchIntegrationCycles {
			// As a shard gives them, with the limit of the commit log of the
			// centroid graph of HFresh at the default of a node.
			tombstones = cyclemanager.NewCallbackGroup("tombstones", logger, 1)
			commitLogs = cyclemanager.NewCallbackGroup("commitlogs", logger, 1)
			cfg.TombstoneCallbacks, cfg.CommitLogCallbacks = tombstones, commitLogs
			cfg.CommitLogThreshold = 100 << 20
		}
		p, err = Open(cfg, uc, store)
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
		if tombstones != nil {
			// The graph takes its deleted centroids out at most once every
			// few minutes, however often the cycle is called.
			tombstones.CycleCallback(never)
			commitLogs.CycleCallback(never)
		}
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
	// In the churn the index is its base alone until the rounds start.
	grownTo := n
	switch {
	case churn > 0:
		grownTo = baseSize
	case reuse:
		// The index is as a run before left it: nothing goes in.
		grownTo = 0
	}
	for inserted < grownTo {
		if !until.IsZero() && time.Now().After(until) {
			shut()
			t.Fatalf("STOP: it is past %s with %d of %d vectors in: the run ends here, with nothing of it alive",
				*benchIntegrationUntil, inserted, n)
		}
		// Once the index has its base, as many callers as asked for add at
		// once, a batch each, as the shared workers of a node do with what a
		// queue holds.
		wasBuilt := p.g != nil
		callers := 1
		if wasBuilt {
			callers = max(*benchIntegrationWriters, 1)
		}
		to := min(inserted+batch*callers, grownTo)
		clock := time.Now()
		adding := enterrors.NewErrorGroupWrapper(logger)
		for from := inserted; from < to; from += batch {
			upto := min(from+batch, to)
			adding.Go(func() error {
				own := make([][]float32, 0, upto-from)
				for _, id := range order[from:upto] {
					own = append(own, data.train[id])
				}
				return p.AddBatch(ctx, order[from:upto], own)
			})
		}
		if err := adding.Wait(); err != nil {
			t.Fatalf("inserts %d to %d: %v", inserted, to, err)
		}
		if !wasBuilt {
			// The index builds its base in the background once it has the
			// vectors for it. The run waits for it, so that every vector
			// after it is an insert, as in the runs it is compared with.
			require.NoError(t, p.settleBase(ctx))
		}
		require.NoError(t, p.Flush())
		took := time.Since(clock)
		switch {
		case !wasBuilt && p.g != nil:
			// The batch that completed the base, and the build of the base.
			result.BaseSeconds = took.Seconds()
			result.Base = p.g.meta.Live
			t.Logf("the base: %d vectors in %d postings, built in %s once the vector that completed it was in", p.g.meta.Live,
				p.g.meta.Postings, took.Round(time.Second))
			lastLog, lastInserted = time.Now(), to
		case wasBuilt:
			insertTime += took
			afterBase += to - inserted
		}
		if to/growCycleEvery != inserted/growCycleEvery {
			cycle()
		}
		inserted = to
		if p.g != nil && (time.Since(lastLog) > 5*time.Minute || inserted == grownTo) {
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
			result.Meta, result.Stats, result.Refreshed = p.g.meta, p.g.stats, p.g.refreshed
			if journal := p.g.journal; journal != nil {
				result.JournalRecords, result.JournalBytes = journal.records, journal.bytes
			}
			p.mu.RUnlock()
			write()
			t.Logf("%d of %d in (%.1f %%): %.0f inserts a second in this stretch, %.0f since the base | %d postings "+
				"given out, %d splits | %d passes over the vectors, %d of %d in the order of their postings | %d "+
				"refreshes of edges ended, %d vectors refreshed", inserted, n,
				100*float64(inserted)/float64(n), stretch.PerSecond, result.PerSecond, result.Meta.Postings,
				result.Meta.Splits, p.layouts.Load(), result.Meta.VectorsLaidOut, result.Meta.Live, p.refreshes.Load(),
				result.Refreshed.Refreshed)
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
	reopened := reuse
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
			Segments: segments(), AsOneSegment: g.merges, VectorGen: g.meta.VectorGen, Slots: int(g.nextSlot),
			VectorsLaidOut: g.meta.VectorsLaidOut, Layouts: p.layouts.Load(),
			CodesInMemory: int64(len(g.st.side.codes)) * 8,
		}
		graphStats, err := g.st.centroids.graph.Stats()
		require.NoError(t, err)
		stage.DeletedCentroids = graphStats.NumTombstones
		var cleaned error
		if tombstones == nil {
			// Nothing keeps the graph: it is cleaned before it is measured,
			// as the experiments do.
			cleaned = g.cleanCentroidGraph()
		}
		if err := g.checkCoverage(); err != nil {
			stage.Coverage = err.Error()
		}
		p.mu.Unlock()
		p.writeMu.Unlock()
		require.NoError(t, cleaned)
		t.Logf("%s: coverage %s | %d segments of postings, left as one %d times since the index was opened | "+
			"generation %d of the vectors, %d of %d in the order of their postings | %d deleted centroids in the graph | "+
			"%.1f MB of routing codes in memory",
			name, stage.Coverage, stage.Segments, stage.AsOneSegment, stage.VectorGen, stage.VectorsLaidOut, stage.Slots,
			stage.DeletedCentroids, float64(stage.CodesInMemory)/1e6)
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
	if reuse {
		// An index a run before left, as it is: what a load reads of its
		// bucket of postings as the cycles left it, and with the bucket as
		// one segment, which the index asks the store for.
		live := make([]bool, n)
		deleted := 0
		for id := range live {
			live[id] = p.ContainsDoc(uint64(id))
			if !live[id] {
				deleted++
			}
		}
		truth, fallbacks := liveTruthOf(t, data, live, k)
		recovered := g.recovered
		result.Recovered = &recovered
		t.Logf("an index a run before left: %d vectors held of %d, %d queries of the ground truth by brute force | "+
			"opened, it applied %d records of its journal again, and built its centroid graph again: %v",
			n-deleted, n, fallbacks, recovered.Records, recovered.GraphRebuilt)
		left := measure("as it was left", truth, live, deleted, nil, "", 0)
		p.mu.Lock()
		if !g.merging {
			g.beginMerge()
		}
		p.mu.Unlock()
		rounds := 0
		for compaction.CycleCallback(func() bool { return false }) {
			rounds++
		}
		p.mu.Lock()
		one := g.endMerge()
		p.mu.Unlock()
		t.Logf("the bucket of postings asked for as one segment: %d rounds of compaction, one segment: %v", rounds, one)
		measure("the bucket of postings as one segment", truth, live, deleted, left, "its bucket was compacted", 0)
		return
	}

	if churn > 0 {
		// The churn of v6.2 (bench-report.md, "v6.2: reparación en el
		// churn"): every round deletes a share of the base at random, inserts
		// as many new vectors, whose searches go through the deleted ones
		// that are still there, and then runs the pass that takes them out.
		perRound := int(*benchIntegrationChurnShare * float64(baseSize))
		require.Positive(t, perRound)
		require.LessOrEqual(t, baseSize+churn*perRound, n, "the train set has no vectors for %d rounds", churn)
		live := make([]bool, n)
		for _, id := range order[:baseSize] {
			live[id] = true
		}
		// edgesPerVector reads every row of edges of the index: the edges of
		// the vectors it holds, on average, how many it holds, and its live
		// postings.
		edgesPerVector := func() (edges float64, held, postings int) {
			p.mu.RLock()
			defer p.mu.RUnlock()
			total := 0
			for q := range uint32(g.meta.Postings) {
				if g.retired[q] {
					continue
				}
				postings++
				_, ids, lists, err := g.readEdges(q)
				require.NoError(t, err)
				for i, id := range ids {
					if g.deleted[id] {
						continue
					}
					held++
					total += len(lists[i])
				}
			}
			return float64(total) / float64(max(held, 1)), held, postings
		}
		edges, held, postings := edgesPerVector()
		t.Logf("the base: %d vectors in %d postings, %.2f edges a vector", held, postings, edges)
		next := baseSize
		for round := 1; round <= churn; round++ {
			var holds []uint64
			for id, is := range live {
				if is {
					holds = append(holds, uint64(id))
				}
			}
			rng := rand.New(rand.NewPCG(6, uint64(round)))
			rng.Shuffle(len(holds), func(i, j int) { holds[i], holds[j] = holds[j], holds[i] })
			victims := holds[:perRound]
			for from := 0; from < len(victims); from += batch {
				require.NoError(t, p.Delete(victims[from:min(from+batch, len(victims))]...))
			}
			for _, id := range victims {
				live[id] = false
			}
			for from := next; from < next+perRound; from += batch {
				to := min(from+batch, next+perRound)
				vectors = vectors[:0]
				for _, id := range order[from:to] {
					vectors = append(vectors, data.train[id])
				}
				if err := p.AddBatch(ctx, order[from:to], vectors); err != nil {
					t.Fatalf("round %d, inserts %d to %d: %v", round, from, to, err)
				}
				require.NoError(t, p.Flush())
				if (to-baseSize)%growCycleEvery == 0 {
					cycle()
				}
			}
			for _, id := range order[next : next+perRound] {
				live[id] = true
			}
			next += perRound

			clock := time.Now()
			require.NoError(t, p.clean(ctx), "round %d: the pass over the deleted vectors", round)
			took := time.Since(clock)
			require.NoError(t, p.Flush())
			cycle()
			edges, held, postings := edgesPerVector()
			p.mu.RLock()
			cleaned, marked := g.cleaned, g.deletedCount
			p.mu.RUnlock()
			require.Zero(t, marked, "round %d: vectors are left marked after the pass", round)
			require.Equal(t, baseSize, held, "round %d: the index holds as many vectors as its base", round)
			result.Rounds = append(result.Rounds, integrationRound{
				Round: round, Deleted: perRound, Inserted: perRound, Cleaned: cleaned, Held: held, Postings: postings,
				EdgesPerVector: edges, PassSeconds: took.Seconds(),
			})
			write()
			t.Logf("round %d: %d deleted, %d inserted, and a pass of %s, with other runs on the machine | so far %d taken "+
				"out, %d edges with a substitute and %d without, %d merges | %d vectors in %d postings, %.2f edges a vector",
				round, perRound, perRound, took.Round(time.Second), cleaned.Removed, cleaned.Substituted, cleaned.Without,
				cleaned.Merges, held, postings, edges)
		}

		truth, fallbacks := liveTruthOf(t, data, live, k)
		t.Logf("the ground truth of the vectors that are left: %d queries by brute force", fallbacks)
		churned := measure("churned", truth, live, churn*perRound, nil, "", 0)
		if *benchIntegrationReopen {
			took := reopen("churned")
			measure("churned, opened again", truth, live, churn*perRound, churned, "the index was closed", took)
		}
		result.DirBytes, err = sizeOnDisk(dir)
		require.NoError(t, err)
		p.mu.RLock()
		churnedMeta := g.meta
		p.mu.RUnlock()

		// The reference: an index whose base is a batch of the vectors that
		// are left, built and read by the same code.
		shut()
		dir += "-reference"
		require.NoError(t, os.MkdirAll(dir, 0o755))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries, "%s is not empty", dir)
		baseSize, counting = 0, false
		for _, is := range live {
			if is {
				baseSize++
			}
		}
		open()
		var ids []uint64
		vectors = vectors[:0]
		clock := time.Now()
		add := func() {
			if len(ids) == 0 {
				return
			}
			if err := p.AddBatch(ctx, ids, vectors); err != nil {
				t.Fatalf("the reference, %d vectors from doc %d: %v", len(ids), ids[0], err)
			}
			require.NoError(t, p.Flush())
			ids, vectors = ids[:0], vectors[:0]
		}
		for _, id := range order {
			if !live[id] {
				continue
			}
			ids, vectors = append(ids, id), append(vectors, data.train[id])
			if len(ids) == batch {
				add()
			}
		}
		add()
		require.NotNil(t, p.g, "the reference has no base")
		g = p.g
		cycle()
		t.Logf("the reference: a batch of the %d vectors that are left, in %d postings, built in %s with other runs on "+
			"the machine", g.meta.Live, g.meta.Postings, time.Since(clock).Round(time.Second))
		measure("reference: a batch of the vectors that are left", truth, live, 0, nil, "", 0)
		reference := g.meta
		result.Reference, result.Meta = &reference, churnedMeta
		write()
		return
	}

	// What the index does in the background is done before it is measured:
	// the refresh of its edges that the last doubling left running, and its
	// vectors laid out if that is due.
	settling := time.Now()
	require.NoError(t, p.Settle(ctx))
	p.mu.RLock()
	result.Refreshed = g.refreshed
	p.mu.RUnlock()
	t.Logf("settled in %s, with other runs on the machine: %d refreshes of edges ended, %d vectors refreshed with %d "+
		"loads, %d lists changed and %d inverse edges entered", time.Since(settling).Round(time.Second),
		p.refreshes.Load(), result.Refreshed.Refreshed, result.Refreshed.SearchLoads, result.Refreshed.ListsChanged,
		result.Refreshed.InverseEdges)
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
		last := again
		// withCodes opens the index of a class that keeps no codes in memory
		// with them, over the same files, measures it, and opens it again as
		// it was: what it ranks its postings by is then read from memory and
		// not from their halos, and it must answer the same.
		withCodes := func(name string) {
			if uc.RQ.CacheMode != ent.CacheModeNone {
				return
			}
			uc.RQ.CacheMode = ent.CacheModeRouting
			took := reopen(name)
			measure(name+", opened with its codes in memory", truth, nil, 0, last, "it was opened with its codes in memory", took)
			uc.RQ.CacheMode = ent.CacheModeNone
			reopen(name + ", with no codes in memory again")
		}
		if !*benchIntegrationReads {
			withCodes("grown")
		}
		if *benchIntegrationReads {
			// What a node that is left alone gets to: every memtable flushed,
			// the store compacted until it finds nothing more to compact,
			// and the vectors in the order of their postings.
			for name, bucket := range store.GetBucketsByName() {
				require.NoError(t, bucket.FlushAndSwitch(), "flush the bucket %s", name)
			}
			// The index asks the store for the bucket of postings as one
			// segment, as it does by itself when enough has been written, and
			// the cycles of the store do it.
			p.mu.Lock()
			if !g.merging {
				g.beginMerge()
			}
			p.mu.Unlock()
			rounds := 0
			for compaction.CycleCallback(func() bool { return false }) {
				rounds++
			}
			p.mu.Lock()
			g.endMerge()
			p.mu.Unlock()
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
			last = measure("grown, compacted and laid out", truth, nil, 0, again, "it was compacted and laid out", 0)
			withCodes("grown, compacted and laid out")
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
		// The pass that takes the deleted vectors out, which runs in the
		// background once enough are marked, is done before they are measured.
		require.NoError(t, p.Settle(ctx))
		require.NoError(t, p.Flush())
		cycle()
		p.mu.RLock()
		cleaned, marked := g.cleaned, g.deletedCount
		p.mu.RUnlock()
		t.Logf("deleted %d vectors, %d %% of them, in %s with the passes that took them out: %d taken out and %d left "+
			"marked, %d edges with a substitute and %d without, %d merges", len(victims), *benchIntegrationDelete,
			time.Since(begin).Round(time.Second), cleaned.Removed, marked, cleaned.Substituted, cleaned.Without,
			cleaned.Merges)
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
