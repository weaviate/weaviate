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

package postingedge

import (
	"bufio"
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// What the indexes that read their postings from disk have in common: the
// keys and the files of the format, the centroid graph with a commit log, a
// posting as a search fetches it, and the search of the index over postings
// that are fetched a round at a time. The index that grows on disk (grow.go)
// is built on it, and so is the batch format of v4.0, which lives with the
// tests (disk_test.go).

const (
	diskBlockMembers byte = iota
	diskBlockEdges
	diskBlockHalo
)

const (
	diskFormatVersion = 1
	diskBucket        = "postingedge_postings"
	diskStoreDir      = "lsm"
	diskGraphDir      = "centroids"
	diskMetaFile      = "meta.json"
	diskMeanFile      = "mean.f32"
	diskLayoutFile    = "layout.u32"
	diskRoutingFile   = "routing.u64"
	diskCentroidFile  = "centroids.f32"
	diskVectorFile    = "vectors.f32"

	// diskNoSlot marks a doc id the index does not hold.
	diskNoSlot = math.MaxUint32
	// diskKeyBytes is the posting, big-endian so that the blocks of a
	// posting are neighbors in the segment, and the block.
	diskKeyBytes = 5
)

func diskKey(p uint32, block byte) []byte {
	key := make([]byte, diskKeyBytes)
	binary.BigEndian.PutUint32(key, p)
	key[4] = block
	return key
}

// diskCodecs tells the quantizer behind the codes of a state and the width of
// the code its endpoints are ranked by, 0 for the full code. The members of a
// posting carry the full code, or that same code of the endpoints: sketchScan
// then, and the format keeps no members on disk (diskMeta.ScanInMemory).
func diskCodecs(st *state) (quantizer *compressionhelpers.CenteredBinaryRotationalQuantizer, routingBits int,
	sketchScan bool, err error,
) {
	if st.scanQuantizer == nil {
		switch only := st.quantizer.(type) {
		case fullCodec:
			return only.CenteredBinaryRotationalQuantizer, 0, false, nil
		case cappedCodec:
			// A capped code is a header word and its sign bits.
			return only.quantizer, (st.side.codeWords - 1) * 64, true, nil
		default:
			return nil, 0, false, errors.Errorf("the postings carry codes of %T, the format knows the full code and its sketches", only)
		}
	}
	full, ok := st.scanQuantizer.(fullCodec)
	if !ok {
		return nil, 0, false, errors.Errorf("the postings carry codes of %T, the format writes the full code", st.scanQuantizer)
	}
	if _, ok := st.quantizer.(cappedCodec); !ok {
		return nil, 0, false, errors.Errorf("the endpoints are ranked by %T, expected a capped code", st.quantizer)
	}
	return full.CenteredBinaryRotationalQuantizer, (st.side.codeWords - 1) * 64, false, nil
}

// writeArray writes values of size bytes each through put, buffered.
func writeArray[T any](path string, size int, put func([]byte, T), arrays ...[]T) (err error) {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()
	w := bufio.NewWriterSize(file, 1<<20)
	buf := make([]byte, size)
	for _, array := range arrays {
		for _, value := range array {
			put(buf, value)
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
	}
	return w.Flush()
}

func putFloat32(buf []byte, value float32) {
	binary.LittleEndian.PutUint32(buf, math.Float32bits(value))
}

// readArray reads a file of values of size bytes each, n of them.
func readArray[T any](path string, size, n int, get func([]byte) T) ([]T, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) != n*size {
		return nil, errors.Errorf("%s holds %d bytes, expected %d", filepath.Base(path), len(raw), n*size)
	}
	out := make([]T, n)
	for i := range out {
		out[i] = get(raw[i*size:])
	}
	return out, nil
}

func getFloat32(buf []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(buf))
}

// openCentroidGraph opens the centroid graph of an index on disk: the graph
// of newCentroidIndex with a commit log under dir. A directory without one
// gives an empty graph that logs what is added to it, and one with a log
// gives back the graph that was logged, link by link. A graph built again
// from the same centroids would not be the same graph.
func openCentroidGraph(ctx context.Context, logger logrus.FieldLogger, dir string, vectors [][]float32,
) (*centroidIndex, error) {
	return openCentroidGraphWith(ctx, logger, dir, vectors, centroidCycles{})
}

// centroidCycles are the cycles of a node that keep a centroid graph as they
// keep any HNSW graph: the one that takes its deleted centroids out for good,
// and the one that condenses and combines the files of its commit log. A
// posting that is split or merged leaves a deleted centroid, and every
// centroid that comes or goes a record in the log. The zero value has
// neither cycle: the experiments, which clean the graph themselves before
// they measure (cleanCentroidGraph) and write an index once.
type centroidCycles struct {
	tombstones cyclemanager.CycleCallbackGroup
	commitLogs cyclemanager.CycleCallbackGroup
	// logThreshold, when positive, is the size past which a file of the
	// commit log is closed and a new one started.
	logThreshold int64
	// everyCycle has the graph take its deleted centroids out in every cycle
	// of the tombstones, and not once in the interval an HNSW graph leaves
	// between two cleanups, which is minutes: for the tests.
	everyCycle bool
}

// openCentroidGraphWith is openCentroidGraph with the cycles that keep the
// graph.
func openCentroidGraphWith(ctx context.Context, logger logrus.FieldLogger, dir string, vectors [][]float32,
	cycles centroidCycles,
) (*centroidIndex, error) {
	if cycles.tombstones == nil {
		cycles.tombstones = cyclemanager.NewCallbackGroupNoop()
	}
	if cycles.commitLogs == nil {
		cycles.commitLogs = cyclemanager.NewCallbackGroupNoop()
	}
	index := &centroidIndex{vectors: vectors}
	index.publish()
	vectorForID := index.publishedVector
	uc := centroidUserConfig(centroidEF)
	if cycles.everyCycle {
		uc.CleanupIntervalSeconds = 0
	}
	graph, err := hnsw.New(hnsw.Config{
		RootPath: dir,
		ID:       "centroids",
		Logger:   logger,
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			if cycles.logThreshold > 0 {
				// Whatever the graph asks for comes after, and wins.
				opts = append([]hnsw.CommitlogOption{hnsw.WithCommitlogThreshold(cycles.logThreshold)}, opts...)
			}
			return hnsw.NewCommitLogger(dir, "centroids", logger, cycles.commitLogs, opts...)
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: vectorForID,
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice,
			_ common.BucketView,
		) ([]float32, error) {
			vec, err := vectorForID(ctx, id)
			if err != nil {
				return nil, err
			}
			container.Slice = append(container.Slice[:0], vec...)
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView { return noopBucketView{} },
		HFreshMode:   true,
	}, uc, cycles.tombstones, nil)
	if err != nil {
		return nil, errors.Wrap(err, "open the centroid graph")
	}
	graph.PostStartup(ctx)
	index.graph = graph
	return index, nil
}

// writeCentroidGraph builds the graph over the centroids into an empty
// directory, with its commit log, and closes it.
func writeCentroidGraph(ctx context.Context, logger logrus.FieldLogger, dir string, vectors [][]float32) error {
	if err := os.Mkdir(dir, 0o755); err != nil {
		return err
	}
	graph, err := openCentroidGraph(ctx, logger, dir, vectors)
	if err != nil {
		return err
	}
	for id, vector := range vectors {
		if err := graph.graph.Add(ctx, uint64(id), vector); err != nil {
			_ = graph.shutdown(ctx)
			return errors.Wrapf(err, "add centroid %d", id)
		}
	}
	if err := graph.graph.Flush(); err != nil {
		_ = graph.shutdown(ctx)
		return errors.Wrap(err, "flush the centroid graph")
	}
	return errors.Wrap(graph.shutdown(ctx), "close the centroid graph")
}

// diskPosting is one posting fetched from the store: its blocks decoded into
// buffers a search reuses.
type diskPosting struct {
	post      posting
	docIDs    []uint64
	codes     []uint64
	edges     []uint32
	edgeStart []uint32
	haloSlots []uint32
	haloCodes []uint64
	// lists and listOf are of a posting of the index that grows (v5.0,
	// grow.go), whose edges come as one list per member: the lists as
	// the store returned them, and which one is of each member.
	lists  [][]byte
	listOf []int32
	// block is the row of edges of a posting of an index with consolidated
	// edges (v5.1), as the store returned it, and row what it decodes to.
	block []byte
	row   growBlock
	// bytes is what the store returned for its blocks.
	bytes [3]int
	// took is what the read of each of its blocks took, from asking the
	// store for it to having it decoded in the posting. The blocks of a
	// round are read at once: these add up to more than the round waits.
	took [3]time.Duration
}

// timedRead runs the read of one block and leaves what it took.
func timedRead(took *time.Duration, read func() error) error {
	clock := time.Now()
	err := read()
	*took = time.Since(clock)
	return err
}

// diskRound holds the postings of one round, fetched together.
type diskRound struct {
	postings []diskPosting
	// inFlight is how many of the reads of the round the budget of its query
	// lets be in flight at once (concurrency.NumWorkers). A query without a
	// budget gets them all at once, and so does a fetch that does not look.
	inFlight int
}

func resized[T any](buf []T, n int) []T {
	if cap(buf) < n {
		return make([]T, n)
	}
	return buf[:n]
}

// scoreHalo estimates the distance to the given endpoints from the codes in
// the halo of the posting, as the sidecar does from its own.
func (dp *diskPosting) scoreHalo(estimator codeEstimator, words int, slots []uint32, dists []float32) error {
	for i, slot := range slots {
		at, found := slices.BinarySearch(dp.haloSlots, slot)
		if !found {
			return errors.Errorf("endpoint %d is not in the halo of the posting that leads to it", slot)
		}
		dist, err := estimator.Distance(dp.haloCodes[at*words : (at+1)*words])
		if err != nil {
			return errors.Wrapf(err, "estimate distance to slot %d", slot)
		}
		dists[i] = dist
	}
	return nil
}

// diskSearchStats is what a search from disk read and where its time went.
type diskSearchStats struct {
	// Work, when set, collects the stats of the search of the index, which
	// costs a second pass over every posting.
	Work *SearchStats
	// Blocks and BlockBytes count the blocks the store returned, and
	// MemberBytes, EdgeBytes and HaloBytes split the bytes by block.
	Blocks      int
	BlockBytes  int
	MemberBytes int
	EdgeBytes   int
	HaloBytes   int
	// Postings and Rounds count the postings loaded and the rounds they
	// came in, the entry postings aside, and Members the members of those
	// postings.
	Postings int
	Rounds   int
	Members  int
	// Loaded lists the postings in the order they were loaded, when
	// KeepLoaded asks for it.
	KeepLoaded bool
	Loaded     []uint32
	// Rescored counts the exact vectors read, one read each.
	Rescored int
	// Budget is the concurrency budget the query ran with, which whoever
	// times the search writes down: the search does not.
	Budget       int
	CentroidTime time.Duration
	FetchTime    time.Duration
	VisitTime    time.Duration
	RescoreTime  time.Duration
	// MemberTime, EdgeTime and HaloTime split by block what the reads of the
	// postings took, each from asking the store to having the block decoded
	// in its posting. The blocks of a round are read at once, so these add
	// up to more than FetchTime, which is what the search waited.
	MemberTime time.Duration
	EdgeTime   time.Duration
	HaloTime   time.Duration
}

// fetchedSearch is the search of the index over postings that are fetched
// from somewhere else than its store, a round at a time: what the index on
// disk of v4.0 and the one that grows on disk of v5.0 have in common.
type fetchedSearch struct {
	// ix holds the search tunables and rescores; st is what the search keeps
	// in memory.
	ix *Index
	st *state
	// rounds pools the *diskRound a search fetches into.
	rounds *sync.Pool
	// fetch reads the given postings together and leaves each ready to be
	// visited, in the order given.
	fetch func(round *diskRound, picks []scored) error
	// halo tells that the endpoints are scored from the halo of the posting
	// that leads to them, whose codes take haloWords words.
	halo      bool
	haloWords int
	// noMembers tells that a posting brings no members block: its members
	// are in memory.
	noMembers bool
	// loaded, when set, is called with every posting right after its visit.
	// The buffers of the posting are reused by the next round.
	loaded func(p uint32, dp *diskPosting)
	// ef, when positive, is the ef of this search in place of the one of the
	// index: the search of an insert for its edges has its own
	// (efConstruction), next to the queries.
	ef int
}

// run searches from the given entry postings. Without any it asks the
// centroid graph, as every search does. The caller answers for the entry
// postings it gives.
func (f *fetchedSearch) run(ctx context.Context, vector []float32, k int, entryIDs []uint64, stats *diskSearchStats,
) ([]uint64, []float32, error) {
	ix, st := f.ix, f.st
	if k < 1 {
		return nil, nil, errors.Errorf("k must be positive, got %d", k)
	}
	if len(vector) != st.dims {
		return nil, nil, errors.Wrapf(common.ErrWrongDimensions,
			"query has length %d, the index holds vectors of length %d", len(vector), st.dims)
	}
	if err := checkValues(vector); err != nil {
		return nil, nil, errors.Wrap(err, "query")
	}
	query := ix.normalized(vector)
	ef := max(int(ix.ef.Load()), k)
	if f.ef > 0 {
		ef = max(f.ef, k)
	}
	rescoreLimit := int(ix.rescoreLimit.Load())
	entries := int(ix.entryPostings.Load())
	batch := int(ix.loadBatch.Load())
	margin := math.Float32frombits(ix.sourceMargin.Load())
	if stats == nil {
		stats = &diskSearchStats{}
	}

	sc := st.borrowScratch()
	defer st.returnScratch(sc)
	round := f.rounds.Get().(*diskRound)
	defer f.rounds.Put(round)

	poolSize := max(rescoreLimit, k)
	sc.pool.reset(poolSize)
	stop := &sc.pool
	if ef != poolSize {
		sc.stop.reset(ef)
		stop = &sc.stop
	}
	search := st.newPostingSearch(sc, query, stop, margin, stats.Work)
	var visiting *diskPosting
	blocks := 2
	if f.noMembers {
		blocks = 1
	}
	if f.halo {
		blocks++
		words := f.haloWords
		search.scoreEndpoints = func(_ *posting, slots []uint32, dists []float32) error {
			return visiting.scoreHalo(search.estimator, words, slots, dists)
		}
	}

	if len(entryIDs) == 0 {
		clock := time.Now()
		var err error
		entryIDs, err = st.centroids.nearest(ctx, query, entries)
		if err != nil {
			return nil, nil, err
		}
		stats.CentroidTime = time.Since(clock)
	}

	// load fetches a set of postings together and visits them in order.
	// All of them are marked before any is visited, so that edges between
	// postings of the same round are not scored.
	load := func(picks []scored) error {
		for _, pick := range picks {
			sc.markLoaded(uint32(pick.id))
		}
		// The reads of a round come out of the budget of the query, which
		// the node shares among the queries it runs at once.
		round.inFlight = concurrency.NumWorkers(ctx, blocks*len(picks), blocks*len(picks))
		clock := time.Now()
		if err := f.fetch(round, picks); err != nil {
			return err
		}
		stats.FetchTime += time.Since(clock)
		clock = time.Now()
		for i, pick := range picks {
			visiting = &round.postings[i]
			if err := search.visit(uint32(pick.id), &visiting.post); err != nil {
				return err
			}
			stats.MemberBytes += visiting.bytes[diskBlockMembers]
			stats.EdgeBytes += visiting.bytes[diskBlockEdges]
			stats.HaloBytes += visiting.bytes[diskBlockHalo]
			stats.MemberTime += visiting.took[diskBlockMembers]
			stats.EdgeTime += visiting.took[diskBlockEdges]
			stats.HaloTime += visiting.took[diskBlockHalo]
			stats.Blocks += blocks
			stats.Members += len(visiting.post.docIDs)
			if f.loaded != nil {
				f.loaded(uint32(pick.id), visiting)
			}
		}
		stats.VisitTime += time.Since(clock)
		stats.Postings += len(picks)
		if stats.KeepLoaded {
			for _, pick := range picks {
				stats.Loaded = append(stats.Loaded, uint32(pick.id))
			}
		}
		return nil
	}

	sc.batch = sc.batch[:0]
	for _, p := range entryIDs {
		sc.batch = append(sc.batch, scored{id: p})
	}
	if err := load(sc.batch); err != nil {
		return nil, nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		sc.selectBatch(batch)
		if len(sc.batch) == 0 || sc.batch[0].dist >= stop.worst() {
			break
		}
		stats.Rounds++
		if err := load(sc.batch); err != nil {
			return nil, nil, err
		}
	}
	stats.BlockBytes = stats.MemberBytes + stats.EdgeBytes + stats.HaloBytes
	if stats.Work != nil {
		// The search of the index counts its rounds where it runs them.
		stats.Work.Rounds = stats.Rounds
	}

	candidates := sc.pool.sorted()
	if rescoreLimit == 0 {
		return topResults(candidates, k)
	}
	clock := time.Now()
	// Every candidate is a read, whether its vector was there or not.
	asked := len(candidates)
	candidates, err := ix.rescore(ctx, query, candidates)
	if err != nil {
		return nil, nil, err
	}
	sc.result.reset(k)
	for _, cand := range candidates {
		sc.result.push(cand.id, cand.dist)
	}
	stats.Rescored = asked
	stats.RescoreTime = time.Since(clock)
	if stats.Work != nil {
		stats.Work.Rescored = asked
	}
	return topResults(sc.result.sorted(), k)
}

// haloState returns the state of an index built in batch with the endpoints
// of its edges ranked by another codec over the same quantizer. The
// postings, with the codes the scan reads, the edges, the centroid graph and
// the posting map are those of the index. The sidecar is a new one, with the
// code of every vector under routing. vectorOf gives the vector of a doc id
// as the index stores it.
func haloState(ctx context.Context, logger logrus.FieldLogger, st *state, routing codec,
	vectorOf func(docID uint64) []float32,
) (*state, error) {
	switch {
	case st == nil:
		return nil, ErrNotBuilt
	case st.postings.lists != nil:
		return nil, errors.New("the index is keyed by doc id: a halo is laid over an index built in batch")
	case st.scanQuantizer != nil:
		return nil, errors.New("the postings have codes of their own already")
	}
	store := st.postings
	n := store.vectors()
	if n == 0 {
		return nil, errors.New("no vector to encode")
	}
	words := len(routing.Encode(vectorOf(store.docIDs[0])))
	codes := make([]uint64, n*words)
	err := parallelChunks(ctx, logger, n, 1024, func(_, start, end int) error {
		for slot := start; slot < end; slot++ {
			code := routing.Encode(vectorOf(store.docIDs[slot]))
			if len(code) != words {
				return errors.Errorf("code of slot %d has %d words, expected %d", slot, len(code), words)
			}
			copy(codes[slot*words:], code)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "encode the endpoints")
	}
	halo := &state{
		dims:          st.dims,
		quantizer:     routing,
		scanQuantizer: st.quantizer,
		centroids:     st.centroids,
		postings:      store,
		side:          &sidecar{codeWords: words, codes: codes, postingOf: st.side.postingOf},
		visited:       visited.NewPool(n),
		build:         st.build,
	}
	postings := store.postings()
	halo.scratch.New = func() any { return newSearchScratch(postings) }
	return halo, nil
}

// unplaced is the posting of a doc id that is in no posting.
const unplaced = math.MaxUint32
