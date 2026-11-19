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
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// The index that grows on disk, v5.0 (bench-report.md, "v5.0: inserts en
// disco"): the incremental path of v2.x (stream_test.go) with the postings in
// the LSM store instead of in memory. It is keyed by doc id, as the streaming
// index is: an edge holds the doc id of its endpoint, and the routing codes
// and the posting map are tables by doc id, so a split moves vectors to new
// postings without touching an edge.
//
// What is on disk, under one directory:
//
//	meta.json      what the index is and how far it has grown
//	mean.f32       the mean of the quantizer
//	centroids.f32  the centroid of every posting id given out, retired ones too
//	centroids/     the centroid graph, with its commit log
//	vectors.f32    the exact vector of doc id d at position d
//	lsm/           four buckets of the store:
//
//	  postings  strategy Set, the one of HFresh's postings. Two rows per
//	            posting, each with one value per member: the members (doc id
//	            and the code the scan reads) and the edges (doc id of the
//	            member and the doc ids its edges lead to). A value is added
//	            without reading or rewriting the row. The list of a member
//	            changes by adding the new one and a tombstone of the old
//	            one. The rows of a posting that was split are never read
//	            again, and the compaction drops them.
//	  map       strategy Replace: doc id -> posting.
//	  routing   strategy Replace: doc id -> the code its neighbors rank it by.
//	  edgedists strategy Replace: doc id -> the exact distance of each of its
//	            edges. Only the inserts read it: an inverse edge enters the
//	            list of a neighbor if it beats its worst edge, and takes the
//	            place its distance gives it.
//
// In memory: the routing codes, the posting map, the centroids and their
// graph. One writer: inserts run one after another, and no search runs next
// to an insert.

const (
	growFormatVersion  = 1
	growBucketPostings = "postingedge_grow_postings"
	growBucketMap      = "postingedge_grow_map"
	growBucketRouting  = "postingedge_grow_routing"
	growBucketDists    = "postingedge_grow_edgedists"
	// growBucketEdges holds the edges of an index with consolidated edges
	// (v5.1): strategy Replace, one value per posting with the lists of all
	// its members, written whole.
	growBucketEdges = "postingedge_grow_edges"
	// growSlotsFile, when an index has it, tells where the exact vector of
	// every doc id is in its vectors file. Only an index that was laid out
	// again has one (reorganizeGrowIndex): one that grows keeps the vector
	// of doc id d at position d.
	growSlotsFile = "slots.u32"
	// growCycleEvery is how many inserts go between two calls to the flush
	// and compaction cycles of the store, which nothing runs in the
	// background here: a run is then the same run every time.
	growCycleEvery = 1000
)

// growMeta describes an index that grows on disk. It is written again every
// time the index is synced.
type growMeta struct {
	Version  int    `json:"version"`
	Distance string `json:"distance"`
	Dims     int    `json:"dims"`
	// Keys is the doc ids the index has room for: [0, Keys).
	Keys             int `json:"keys"`
	ScanCodeWords    int `json:"scanCodeWords"`
	RoutingBits      int `json:"routingBits"`
	RoutingCodeWords int `json:"routingCodeWords"`
	// Neighbors is how many neighbors the search of an insert keeps, and
	// Threshold the size past which a posting splits.
	Neighbors int    `json:"neighbors"`
	Threshold int    `json:"threshold"`
	Seed      uint64 `json:"seed"`
	// Postings is the posting ids given out, the retired ones too. Splits and
	// SplitsFailed seed the next split, as in the streaming index.
	Postings     int `json:"postings"`
	Splits       int `json:"splits"`
	SplitsFailed int `json:"splitsFailed"`
	Live         int `json:"live"`
	// Consolidate, when positive, tells that the edges are consolidated
	// (v5.1, bench-report.md, "v5.1: aristas consolidadas"): the changes to
	// the edges of a posting wait in memory, and its row of edges is written
	// whole, as one value of the bucket of edges, once it has this many, when
	// the posting splits and when the index closes. With 0 every change is
	// a value of its own in the bucket of postings, as in v5.0.
	Consolidate int `json:"consolidate,omitempty"`
}

// growIO counts reads or writes: how many, and their bytes.
type growIO struct {
	N     int64 `json:"n"`
	Bytes int64 `json:"bytes"`
}

func (c *growIO) add(n, bytes int) {
	c.N += int64(n)
	c.Bytes += int64(bytes)
}

// growStats counts what the inserts did. Writes are what was handed to the
// store, value by value, in bytes of value. Reads are what the store handed
// back, row by row, and the exact vectors read from their file.
type growStats struct {
	Inserted     int `json:"inserted"`
	Splits       int `json:"splits"`
	SplitsFailed int `json:"splitsFailed"`
	// InverseChecks counts the neighbors whose list an insert looked at,
	// InverseEdges those it entered, and InversePostings the distinct
	// postings those lists live in, added up over the inserts.
	InverseChecks   int `json:"inverseChecks"`
	InverseEdges    int `json:"inverseEdges"`
	InverseAlready  int `json:"inverseAlready,omitempty"`
	InversePostings int `json:"inversePostings"`
	// InverseListReads counts the lists of neighbors the search of the
	// insert had not left behind, read again from the store.
	InverseListReads int `json:"inverseListReads"`

	// The search of every insert for its edges.
	SearchLoads    int    `json:"searchLoads"`
	SearchRounds   int    `json:"searchRounds"`
	SearchRows     growIO `json:"searchRows"`
	SearchVectors  growIO `json:"searchVectors"`
	InverseDists   growIO `json:"inverseDistReads"`
	InverseVectors growIO `json:"inverseVectorReads"`
	SplitRows      growIO `json:"splitRowReads"`
	SplitVectors   growIO `json:"splitVectorReads"`

	MemberRow     growIO `json:"memberRow"`
	MemberMap     growIO `json:"memberMap"`
	MemberRouting growIO `json:"memberRouting"`
	MemberVector  growIO `json:"memberVector"`
	OwnEdges      growIO `json:"ownEdges"`
	OwnDists      growIO `json:"ownDists"`
	InverseLists  growIO `json:"inverseLists"`
	InverseTombs  growIO `json:"inverseTombstones"`
	InverseDistsW growIO `json:"inverseDistWrites"`
	SplitRowsW    growIO `json:"splitRowWrites"`
	SplitMap      growIO `json:"splitMapWrites"`
	SplitCentroid growIO `json:"splitCentroidWrites"`

	// What the store itself counted as read, in the mode it counts in, by
	// the step of the insert: its search, its inverse edges and its split.
	// Zero unless the index was given the counter.
	StoreSearch  growIO `json:"storeSearchReads"`
	StoreInverse growIO `json:"storeInverseReads"`
	StoreSplit   growIO `json:"storeSplitReads"`

	// With consolidated edges: the rows of edges written whole, by what had
	// them written, the rows read to be written again, the distances written
	// with them, and what waited in memory at its most.
	RowWrites        growIO `json:"rowWrites"`
	RowWritesFull    int    `json:"rowWritesFull"`
	RowWritesSplit   int    `json:"rowWritesSplit"`
	RowWritesClose   int    `json:"rowWritesClose"`
	RowReads         growIO `json:"rowReads"`
	RowDists         growIO `json:"rowDistWrites"`
	RowDeletes       int    `json:"rowDeletes"`
	StoreRows        growIO `json:"storeRowReads"`
	PendingPeak      int64  `json:"pendingPeakMembers"`
	PendingPeakBytes int64  `json:"pendingPeakBytes"`
	PendingPeakPosts int    `json:"pendingPeakPostings"`

	AssignTime  time.Duration `json:"assignNs"`
	SearchTime  time.Duration `json:"searchNs"`
	WriteTime   time.Duration `json:"writeNs"`
	InverseTime time.Duration `json:"inverseNs"`
	SplitTime   time.Duration `json:"splitNs"`
	CycleTime   time.Duration `json:"cycleNs"`
	RowTime     time.Duration `json:"rowNs"`
}

// growPending is what waits in memory of a posting with consolidated edges:
// the list of every member that changed since its row of edges was written,
// a new member or one that an inverse edge entered, with the distances of
// its edges.
type growPending struct {
	// order has the members in the order of their first change: the new
	// ones are in the order they came, which is their order in the row of
	// members.
	order   []uint64
	lists   map[uint64][]uint32
	dists   map[uint64][]float32
	changes int
	bytes   int64
}

// growKept is the part of a posting the search of an insert leaves behind
// for its inverse edges: who its members are and where their edges lead.
type growKept struct {
	docIDs    []uint64
	edgeStart []uint32
	edges     []uint32
}

// growIndex is an index that grows on disk.
type growIndex struct {
	dir      string
	meta     growMeta
	logger   logrus.FieldLogger
	provider distancer.Provider
	// ix holds the search tunables and rescores, reading the vectors
	// through readVector. It is never built.
	ix *Index
	// st has what a search keeps in memory: the routing codes and the
	// posting map by doc id, the centroids and their graph.
	st *state

	lsm      *lsmkv.Store
	postings *lsmkv.Bucket
	posts    *lsmkv.Bucket
	routing  *lsmkv.Bucket
	dists    *lsmkv.Bucket
	// edges is the bucket of the rows of edges, nil unless the edges are
	// consolidated, and pending what waits of every posting.
	edges          *lsmkv.Bucket
	pending        map[uint32]*growPending
	pendingMembers int64
	pendingBytes   int64
	flush          cyclemanager.CycleCallbackGroup
	compaction     cyclemanager.CycleCallbackGroup
	vectors        *os.File
	centroids      *os.File
	rounds         sync.Pool // *diskRound

	live []bool   // by doc id
	size []uint32 // by posting: its members
	// slotOf, when set, is the position of the exact vector of every doc id
	// in the vectors file. Such an index is searched, and does not grow.
	slotOf []uint32
	// retired tells the postings that were split. The compaction of the
	// store asks, from wherever it runs.
	retiredMu sync.RWMutex
	retired   []bool

	sums  []float64
	l2    distancer.Provider
	stats growStats
	// storeReads, when set, returns what the store has counted as read so
	// far. An insert then tells what each of its steps read.
	storeReads func() (reads, bytes uint64, err error)
	// work, when set, takes the work of the search of every insert.
	work *SearchStats
	// lastPosting and lastEdges are the posting and the edges the last
	// insert gave its vector.
	lastPosting uint32
	lastEdges   []uint32

	// What one insert keeps while it runs: the edges of the postings its
	// search loaded, and the exact vectors its rescore read.
	kept   []growKept
	keptAt map[uint32]int
	// touched has the postings whose edges the insert that runs changed.
	touched   []uint32
	capturing atomic.Bool
	captureMu sync.Mutex
	captured  map[uint64][]float32
	// vectorReads counts the exact vectors read from the file.
	vectorReads atomic.Int64
	sinceCycle  int
}

func growDocKey(docID uint64) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(docID))
	return key
}

// growMemberValue is a member of a posting: its doc id and the code the scan
// reads, as a block of members of v4.0 holds them.
func growMemberValue(docID uint64, code []uint64) []byte {
	value := make([]byte, 8*(1+len(code)))
	binary.LittleEndian.PutUint64(value, docID)
	for i, word := range code {
		binary.LittleEndian.PutUint64(value[8*(1+i):], word)
	}
	return value
}

// growEdgesValue is the list of edges of a member: its doc id and those its
// edges lead to, nearest first. A member without edges has a list too.
func growEdgesValue(docID uint64, edges []uint32) []byte {
	value := make([]byte, 4*(1+len(edges)))
	binary.LittleEndian.PutUint32(value, uint32(docID))
	for i, endpoint := range edges {
		binary.LittleEndian.PutUint32(value[4*(1+i):], endpoint)
	}
	return value
}

func growDistsValue(dists []float32) []byte {
	value := make([]byte, 4*len(dists))
	for i, dist := range dists {
		putFloat32(value[4*i:], dist)
	}
	return value
}

func growUint32Value(x uint32) []byte {
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, x)
	return value
}

func growCodeValue(code []uint64) []byte {
	value := make([]byte, 8*len(code))
	for i, word := range code {
		binary.LittleEndian.PutUint64(value[8*i:], word)
	}
	return value
}

// growOpen says how the store of an index that grows is opened.
type growOpen struct {
	// metrics and pread make the store count what it reads.
	metrics *lsmkv.Metrics
	pread   bool
	// leveled leaves the bucket of postings the compaction the store has by
	// default, which only merges two segments of the same level: what was
	// replaced piles up between compactions. Without it the bucket compacts
	// as the postings bucket of HFresh does, the segments left over too,
	// which keeps it close to one segment by writing it again and again.
	leveled bool
}

// growPostingKey is the key of a posting in the bucket of edges.
func growPostingKey(p uint32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, p)
	return key
}

// growEdgesBlock is the row of edges of a posting as one value: how many
// members, the doc id and the degree of each, in the order of the row of
// members, and then their edges, member after member.
func growEdgesBlock(ids []uint64, lists [][]uint32) ([]byte, error) {
	total := 0
	for _, list := range lists {
		if len(list) > math.MaxUint8 {
			return nil, errors.Errorf("a list of %d edges, a row holds up to %d a member", len(list), math.MaxUint8)
		}
		total += len(list)
	}
	block := make([]byte, 4+5*len(ids)+4*total)
	binary.LittleEndian.PutUint32(block, uint32(len(ids)))
	at := 4 + 5*len(ids)
	for i, id := range ids {
		binary.LittleEndian.PutUint32(block[4+5*i:], uint32(id))
		block[4+5*i+4] = uint8(len(lists[i]))
		for _, endpoint := range lists[i] {
			binary.LittleEndian.PutUint32(block[at:], endpoint)
			at += 4
		}
	}
	return block, nil
}

// growBlock is a row of edges decoded: the members it has a list of, in its
// order, and where the list of each starts.
type growBlock struct {
	ids   []uint32
	start []uint32
	edges []uint32
}

func (b *growBlock) decode(block []byte) error {
	if len(block) < 4 {
		return errors.Errorf("a row of edges of %d bytes", len(block))
	}
	n := int(binary.LittleEndian.Uint32(block))
	if len(block) < 4+5*n {
		return errors.Errorf("a row of edges of %d bytes for %d members", len(block), n)
	}
	b.ids, b.start = resized(b.ids, n), resized(b.start, n+1)
	b.start[0] = 0
	for i := range n {
		b.ids[i] = binary.LittleEndian.Uint32(block[4+5*i:])
		b.start[i+1] = b.start[i] + uint32(block[4+5*i+4])
	}
	rest := block[4+5*n:]
	if len(rest) != 4*int(b.start[n]) {
		return errors.Errorf("a row of edges with %d bytes of edges, its degrees add up to %d edges", len(rest), b.start[n])
	}
	b.edges = resized(b.edges, int(b.start[n]))
	for i := range b.edges {
		b.edges[i] = binary.LittleEndian.Uint32(rest[4*i:])
	}
	return nil
}

// growConfig is what an index that grows is written with.
type growConfig struct {
	// keys is the doc ids the index has room for.
	keys int
	// neighbors is how many neighbors the search of an insert keeps, the
	// edges of the batch build.
	neighbors int
	// threshold is the size past which a posting splits.
	threshold int
	seed      uint64
	// consolidate, when positive, has the edges consolidated: the changes
	// a row of edges is written after (growMeta.Consolidate).
	consolidate int
}

// writeGrowIndex writes an index built in batch to dir, which must exist and
// be empty, in the format that grows: the base the inserts start from. st is
// the state of the index, keyed by slot. vectorOf gives the vector of a doc
// id as the index stores it.
func writeGrowIndex(ctx context.Context, logger logrus.FieldLogger, dir string, st *state, cfg growConfig,
	provider distancer.Provider, vectorOf func(docID uint64) []float32,
) (err error) {
	if st == nil {
		return ErrNotBuilt
	}
	store := st.postings
	if store.lists != nil {
		return errors.New("the index is keyed by doc id already: the base is an index built in batch")
	}
	quantizer, routingBits, err := diskCodecs(st)
	if err != nil {
		return err
	}
	if entries, err := os.ReadDir(dir); err != nil {
		return err
	} else if len(entries) > 0 {
		return errors.Errorf("%s is not empty", dir)
	}
	n, postings := store.vectors(), store.postings()
	switch {
	case n == 0:
		return errors.New("no vector to write")
	case cfg.keys < n:
		return errors.Errorf("room for %d doc ids, the base holds %d vectors", cfg.keys, n)
	case cfg.threshold < 2:
		return errors.Errorf("a split threshold of %d leaves nothing to split", cfg.threshold)
	}
	for _, docID := range store.docIDs {
		if docID >= uint64(cfg.keys) {
			return errors.Errorf("doc id %d is past the %d keys", docID, cfg.keys)
		}
	}
	g := &growIndex{dir: dir, logger: logger, provider: provider}
	g.meta = growMeta{
		Version: growFormatVersion, Distance: provider.Type(), Dims: st.dims, Keys: cfg.keys,
		ScanCodeWords: store.codeWords, RoutingBits: routingBits, RoutingCodeWords: st.side.codeWords,
		Neighbors: cfg.neighbors, Threshold: cfg.threshold, Seed: cfg.seed, Postings: postings, Live: n,
		Consolidate: cfg.consolidate,
	}
	path := func(name string) string { return filepath.Join(dir, name) }
	if err := writeArray(path(diskMeanFile), 4, putFloat32, quantizer.Data().Mean); err != nil {
		return errors.Wrap(err, "write the mean of the quantizer")
	}
	if err := writeArray(path(diskCentroidFile), 4, putFloat32, st.centroids.vectors...); err != nil {
		return errors.Wrap(err, "write the centroids")
	}
	// The graph over the centroids is built here, with a commit log, and
	// not copied from the index, whose graph logs nothing.
	if err := writeCentroidGraph(ctx, logger, path(diskGraphDir), st.centroids.vectors); err != nil {
		return err
	}

	if err := g.openStore(ctx, growOpen{}); err != nil {
		return err
	}
	defer func() {
		if shut := g.lsm.Shutdown(ctx); err == nil {
			err = errors.Wrap(shut, "close the store")
		}
	}()
	if g.vectors, err = os.OpenFile(path(diskVectorFile), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return err
	}
	defer func() {
		if closed := g.vectors.Close(); err == nil {
			err = closed
		}
	}()

	raw := make([]byte, 4*st.dims)
	var dists []float32
	var edges []uint32
	for p := range postings {
		from, to := store.start[p], store.start[p+1]
		members := make([][]byte, 0, to-from)
		lists := make([][]byte, 0, to-from)
		rowLists := make([][]uint32, 0, to-from)
		for slot := from; slot < to; slot++ {
			docID := store.docIDs[slot]
			vector := vectorOf(docID)
			members = append(members, growMemberValue(docID, store.codes[int(slot)*store.codeWords:int(slot+1)*store.codeWords]))
			edges, dists = edges[:0], dists[:0]
			for _, endpoint := range store.edgesOf(slot) {
				target := store.docIDs[endpoint]
				dist, err := provider.SingleDist(vector, vectorOf(target))
				if err != nil {
					return err
				}
				edges, dists = append(edges, uint32(target)), append(dists, dist)
			}
			lists = append(lists, growEdgesValue(docID, edges))
			rowLists = append(rowLists, slices.Clone(edges))
			if err := g.posts.Put(growDocKey(docID), growUint32Value(uint32(p))); err != nil {
				return errors.Wrap(err, "write the posting map")
			}
			if err := g.routing.Put(growDocKey(docID), growCodeValue(st.side.code(slot))); err != nil {
				return errors.Wrap(err, "write a routing code")
			}
			if err := g.dists.Put(growDocKey(docID), growDistsValue(dists)); err != nil {
				return errors.Wrap(err, "write the distances of the edges")
			}
			for i, x := range vector {
				putFloat32(raw[4*i:], x)
			}
			if _, err := g.vectors.WriteAt(raw, int64(docID)*int64(len(raw))); err != nil {
				return errors.Wrapf(err, "write the vector of doc %d", docID)
			}
		}
		if err := g.postings.SetAdd(diskKey(uint32(p), diskBlockMembers), members); err != nil {
			return errors.Wrapf(err, "write the members of posting %d", p)
		}
		if cfg.consolidate > 0 {
			block, err := growEdgesBlock(store.docIDs[from:to], rowLists)
			if err != nil {
				return err
			}
			if err := g.edges.Put(growPostingKey(uint32(p)), block); err != nil {
				return errors.Wrapf(err, "write the row of edges of posting %d", p)
			}
		} else if err := g.postings.SetAdd(diskKey(uint32(p), diskBlockEdges), lists); err != nil {
			return errors.Wrapf(err, "write the edges of posting %d", p)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	// The file has room for every key: a vector is read at its doc id.
	if err := g.vectors.Truncate(int64(cfg.keys) * int64(len(raw))); err != nil {
		return err
	}
	if err := g.flushAll(); err != nil {
		return err
	}
	return g.writeMeta()
}

func (g *growIndex) writeMeta() error {
	raw, err := json.MarshalIndent(g.meta, "", "  ")
	if err != nil {
		return err
	}
	return errors.Wrap(os.WriteFile(filepath.Join(g.dir, diskMetaFile), raw, 0o644), "write the meta")
}

// skipKey tells the store that a row is of a posting that was split: nothing
// reads it again, and a flush or a compaction can leave it out.
func (g *growIndex) skipKey(key []byte, _ context.Context) (bool, error) {
	if len(key) != diskKeyBytes {
		// Skipping is an optimization: a key that is not understood stays.
		return false, errors.Errorf("a key of %d bytes in the bucket of postings", len(key))
	}
	p := binary.BigEndian.Uint32(key)
	g.retiredMu.RLock()
	defer g.retiredMu.RUnlock()
	return int(p) < len(g.retired) && g.retired[p], nil
}

// openStore opens the four buckets. The flush and compaction cycles of the
// store are the index's to call (cycle): nothing runs them in the
// background. A memtable is flushed past the default threshold of the store,
// and never because of the time it has been dirty.
func (g *growIndex) openStore(ctx context.Context, opts growOpen) error {
	root := filepath.Join(g.dir, diskStoreDir)
	g.flush = cyclemanager.NewCallbackGroup("flush", g.logger, 1)
	g.compaction = cyclemanager.NewCallbackGroup("compaction", g.logger, 1)
	store, err := lsmkv.New(root, root, g.logger, opts.metrics, nil, g.compaction, cyclemanager.NewCallbackGroupNoop(), g.flush)
	if err != nil {
		return errors.Wrap(err, "open the store")
	}
	g.lsm = store
	common := []lsmkv.BucketOption{
		lsmkv.WithPread(opts.pread), lsmkv.WithForceCompaction(!opts.leveled), lsmkv.WithDirtyThreshold(1000 * time.Hour),
	}
	err = store.CreateOrLoadBucket(ctx, growBucketPostings, append([]lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategySetCollection), lsmkv.WithShouldSkipKeyFunction(g.skipKey),
	}, common...)...)
	if err != nil {
		_ = store.Shutdown(ctx)
		return errors.Wrap(err, "open the bucket of postings")
	}
	replace := []string{growBucketMap, growBucketRouting, growBucketDists}
	if g.meta.Consolidate > 0 {
		replace = append(replace, growBucketEdges)
	}
	for _, name := range replace {
		err = store.CreateOrLoadBucket(ctx, name, append([]lsmkv.BucketOption{lsmkv.WithStrategy(lsmkv.StrategyReplace)}, common...)...)
		if err != nil {
			_ = store.Shutdown(ctx)
			return errors.Wrapf(err, "open the bucket %s", name)
		}
	}
	g.postings, g.posts = store.Bucket(growBucketPostings), store.Bucket(growBucketMap)
	g.routing, g.dists = store.Bucket(growBucketRouting), store.Bucket(growBucketDists)
	if g.meta.Consolidate > 0 {
		g.edges = store.Bucket(growBucketEdges)
		g.pending = make(map[uint32]*growPending)
	}
	return nil
}

func (g *growIndex) buckets() []*lsmkv.Bucket {
	buckets := []*lsmkv.Bucket{g.postings, g.posts, g.routing, g.dists}
	if g.edges != nil {
		buckets = append(buckets, g.edges)
	}
	return buckets
}

// flushAll writes every memtable to a segment.
func (g *growIndex) flushAll() error {
	for _, bucket := range g.buckets() {
		if err := bucket.FlushAndSwitch(); err != nil {
			return errors.Wrap(err, "flush a bucket")
		}
	}
	return nil
}

// cycle calls the flush and the compaction cycles of the store once, as its
// background routines would.
func (g *growIndex) cycle() {
	never := func() bool { return false }
	g.flush.CycleCallback(never)
	g.compaction.CycleCallback(never)
}

// compactAll flushes everything and compacts until the store finds nothing
// more to compact, which leaves every bucket in one segment unless the store
// was opened with its leveled compaction. It returns the rounds that took.
func (g *growIndex) compactAll() (int, error) {
	if err := g.flushAll(); err != nil {
		return 0, err
	}
	rounds := 0
	for g.compaction.CycleCallback(func() bool { return false }) {
		rounds++
	}
	return rounds, nil
}

// openGrowIndex opens an index that grows. uc carries the search tunables.
func openGrowIndex(ctx context.Context, logger logrus.FieldLogger, dir string, provider distancer.Provider,
	uc UserConfig, opts growOpen,
) (_ *growIndex, err error) {
	path := func(name string) string { return filepath.Join(dir, name) }
	raw, err := os.ReadFile(path(diskMetaFile))
	if err != nil {
		return nil, errors.Wrap(err, "read the meta")
	}
	g := &growIndex{dir: dir, logger: logger, provider: provider, l2: distancer.NewL2SquaredProvider()}
	if err := json.Unmarshal(raw, &g.meta); err != nil {
		return nil, errors.Wrap(err, "parse the meta")
	}
	meta := &g.meta
	switch {
	case meta.Version != growFormatVersion:
		return nil, errors.Errorf("format version %d, this reader reads %d", meta.Version, growFormatVersion)
	case meta.Distance != provider.Type():
		return nil, errors.Errorf("the index was written for distance %q, opened with %q", meta.Distance, provider.Type())
	case meta.Dims <= 0 || meta.Keys <= 0 || meta.Postings <= 0:
		return nil, errors.Errorf("meta of an empty index: %d dims, %d keys, %d postings", meta.Dims, meta.Keys, meta.Postings)
	}
	defer func() {
		if err != nil {
			_ = g.close(ctx)
		}
	}()

	mean, err := readArray(path(diskMeanFile), 4, meta.Dims, getFloat32)
	if err != nil {
		return nil, errors.Wrap(err, "read the mean of the quantizer")
	}
	quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(meta.Dims,
		compressionhelpers.DefaultFastRotationSeed, mean, provider)
	if err != nil {
		return nil, errors.Wrap(err, "restore the quantizer")
	}
	scan := fullCodec{quantizer}
	var routing codec = scan
	if meta.RoutingBits > 0 {
		if routing, err = scan.withBits(meta.RoutingBits); err != nil {
			return nil, err
		}
	}

	flat, err := readArray(path(diskCentroidFile), 4, meta.Postings*meta.Dims, getFloat32)
	if err != nil {
		return nil, errors.Wrap(err, "read the centroids")
	}
	centroids := make([][]float32, meta.Postings)
	for p := range centroids {
		centroids[p] = flat[p*meta.Dims : (p+1)*meta.Dims : (p+1)*meta.Dims]
	}
	graph, err := openCentroidGraph(ctx, logger, path(diskGraphDir), centroids)
	if err != nil {
		return nil, err
	}
	g.st = &state{dims: meta.Dims, quantizer: routing, centroids: graph}
	if meta.RoutingBits > 0 {
		g.st.scanQuantizer = scan
	}
	if g.centroids, err = os.OpenFile(path(diskCentroidFile), os.O_RDWR, 0o644); err != nil {
		return nil, errors.Wrap(err, "open the centroids")
	}
	if g.vectors, err = os.OpenFile(path(diskVectorFile), os.O_RDWR, 0o644); err != nil {
		return nil, errors.Wrap(err, "open the vectors")
	}
	if _, err := os.Stat(path(growSlotsFile)); err == nil {
		if g.slotOf, err = readArray(path(growSlotsFile), 4, meta.Keys, binary.LittleEndian.Uint32); err != nil {
			return nil, errors.Wrap(err, "read where the vectors are")
		}
	}
	if err := g.openStore(ctx, opts); err != nil {
		return nil, err
	}

	// What stays in memory, read from its buckets: the posting map, and with
	// it who is live and how large every posting is, and the routing codes.
	side := &sidecar{
		codeWords: meta.RoutingCodeWords, codes: make([]uint64, meta.Keys*meta.RoutingCodeWords),
		postingOf: make([]uint32, meta.Keys),
	}
	for i := range side.postingOf {
		side.postingOf[i] = unplaced
	}
	g.live, g.size = make([]bool, meta.Keys), make([]uint32, meta.Postings)
	live := 0
	err = growScan(g.posts, func(key, value []byte) error {
		if len(key) != 4 || len(value) != 4 {
			return errors.Errorf("an entry of the posting map with a key of %d bytes and a value of %d", len(key), len(value))
		}
		docID, p := binary.BigEndian.Uint32(key), binary.LittleEndian.Uint32(value)
		if int(docID) >= meta.Keys || int(p) >= meta.Postings {
			return errors.Errorf("the map puts doc %d in posting %d, with %d keys and %d postings", docID, p, meta.Keys, meta.Postings)
		}
		side.postingOf[docID], g.live[docID] = p, true
		g.size[p]++
		live++
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "read the posting map")
	}
	if live != meta.Live {
		return nil, errors.Errorf("the map holds %d docs, the meta says %d are live", live, meta.Live)
	}
	coded := 0
	err = growScan(g.routing, func(key, value []byte) error {
		words := meta.RoutingCodeWords
		if len(key) != 4 || len(value) != 8*words {
			return errors.Errorf("a routing code with a key of %d bytes and a value of %d", len(key), len(value))
		}
		docID := binary.BigEndian.Uint32(key)
		if int(docID) >= meta.Keys || !g.live[docID] {
			return errors.Errorf("a routing code for doc %d, which is not in the map", docID)
		}
		for i := range words {
			side.codes[int(docID)*words+i] = binary.LittleEndian.Uint64(value[8*i:])
		}
		coded++
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "read the routing codes")
	}
	if coded != live {
		return nil, errors.Errorf("%d routing codes for %d docs", coded, live)
	}
	g.retired = make([]bool, meta.Postings)
	for p, size := range g.size {
		g.retired[p] = size == 0
	}

	// Every split adds two postings, and there is at most one per insert.
	capacity := meta.Postings + 2*(meta.Keys-live) + 2
	g.st.postings = &postingStore{codeWords: meta.ScanCodeWords, lists: []postingList{}}
	g.st.side = side
	g.st.visited = visited.NewPool(meta.Keys)
	g.st.scratch.New = func() any { return newSearchScratch(capacity) }
	g.rounds.New = func() any { return &diskRound{} }
	g.sums = make([]float64, meta.Dims)
	g.keptAt = make(map[uint32]int)
	g.captured = make(map[uint64][]float32)

	g.ix, err = New(Config{
		ID:                           "grow",
		RootPath:                     dir,
		Logger:                       logger,
		DistanceProvider:             provider,
		TempVectorForIDWithViewThunk: g.readVector,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
	}, uc)
	if err != nil {
		return nil, errors.Wrap(err, "configure the search")
	}
	return g, nil
}

// growScan calls visit with every entry of a bucket of strategy Replace. The
// key and the value are only valid during the call.
func growScan(bucket *lsmkv.Bucket, visit func(key, value []byte) error) error {
	cursor := bucket.Cursor()
	defer cursor.Close()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if err := visit(key, value); err != nil {
			return err
		}
	}
	return nil
}

// sync leaves on disk what the index has in memory beyond the store: the
// commit log of the centroid graph and the meta. The store logs its writes
// as they come.
func (g *growIndex) sync() error {
	if err := g.st.centroids.graph.Flush(); err != nil {
		return errors.Wrap(err, "flush the centroid graph")
	}
	for _, bucket := range g.buckets() {
		if err := bucket.WriteWAL(); err != nil {
			return errors.Wrap(err, "write the log of a bucket")
		}
	}
	return g.writeMeta()
}

// close syncs the index and releases what it holds. It must not run next to
// a search or an insert.
func (g *growIndex) close(ctx context.Context) error {
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	if g.st != nil && g.st.centroids != nil && g.lsm != nil && g.ix != nil {
		keep(g.flushPending())
		keep(g.sync())
	}
	if g.ix != nil {
		keep(g.ix.Shutdown(ctx))
	}
	if g.st != nil && g.st.centroids != nil {
		keep(g.st.centroids.shutdown(ctx))
	}
	if g.lsm != nil {
		keep(g.lsm.Shutdown(ctx))
	}
	for _, file := range []*os.File{g.vectors, g.centroids} {
		if file != nil {
			keep(file.Close())
		}
	}
	return first
}

// exact reads the exact vector of a doc from the file into buf, which is
// grown as needed, and returns it.
func (g *growIndex) exact(docID uint64, raw []byte, into []float32) ([]byte, []float32, error) {
	dims := g.meta.Dims
	if cap(raw) < 4*dims {
		raw = make([]byte, 4*dims)
	}
	raw = raw[:4*dims]
	slot := docID
	if g.slotOf != nil {
		if g.slotOf[docID] == diskNoSlot {
			return raw, into, errors.Errorf("doc %d has no vector in the file", docID)
		}
		slot = uint64(g.slotOf[docID])
	}
	if _, err := g.vectors.ReadAt(raw, int64(slot)*int64(4*dims)); err != nil {
		return raw, into, errors.Wrapf(err, "read the vector of doc %d", docID)
	}
	if cap(into) < dims {
		into = make([]float32, dims)
	}
	into = into[:dims]
	for i := range into {
		into[i] = getFloat32(raw[4*i:])
	}
	g.vectorReads.Add(1)
	return raw, into, nil
}

// readVector reads the exact vector of a doc for a rescore, with one
// positional read. While an insert runs it keeps a copy: the insert needs
// the vectors of its neighbors again.
func (g *growIndex) readVector(_ context.Context, docID uint64, container *common.VectorSlice, _ common.BucketView,
) ([]float32, error) {
	if docID >= uint64(len(g.live)) || !g.live[docID] {
		return nil, storobj.NewErrNotFoundf(docID, "the index does not hold it")
	}
	var err error
	container.Buff, container.Slice, err = g.exact(docID, container.Buff, container.Slice)
	if err != nil {
		return nil, err
	}
	if g.capturing.Load() {
		g.captureMu.Lock()
		g.captured[docID] = slices.Clone(container.Slice)
		g.captureMu.Unlock()
	}
	return container.Slice, nil
}

// row reads one row of a posting: the values of its members, or the lists of
// their edges with the lists that were replaced left out.
func (g *growIndex) row(p uint32, block byte) ([][]byte, error) {
	var values [][]byte
	var err error
	if block == diskBlockMembers {
		// No member is ever taken out of a row, so its values are as the
		// store holds them, and in the order they were added.
		values, err = g.postings.SetRawList(diskKey(p, block))
	} else {
		values, err = g.postings.SetList(diskKey(p, block))
	}
	return values, errors.Wrapf(err, "read row %d of posting %d", block, p)
}

func (g *growIndex) fetchMembers(p uint32, dp *diskPosting) error {
	values, err := g.row(p, diskBlockMembers)
	if err != nil {
		return err
	}
	words := g.meta.ScanCodeWords
	n := len(values)
	if n != int(g.size[p]) {
		return errors.Errorf("the row of posting %d holds %d members, the index counts %d", p, n, g.size[p])
	}
	dp.docIDs, dp.codes = resized(dp.docIDs, n), resized(dp.codes, n*words)
	bytes := 0
	for i, value := range values {
		if len(value) != 8*(1+words) {
			return errors.Errorf("a member of posting %d takes %d bytes, expected %d", p, len(value), 8*(1+words))
		}
		dp.docIDs[i] = binary.LittleEndian.Uint64(value)
		for w := range words {
			dp.codes[i*words+w] = binary.LittleEndian.Uint64(value[8*(1+w):])
		}
		bytes += len(value)
	}
	dp.bytes[diskBlockMembers] = bytes
	return nil
}

func (g *growIndex) fetchLists(p uint32, dp *diskPosting) error {
	if g.edges != nil {
		// One value with the whole row, the last one written.
		block, err := g.edges.Get(growPostingKey(p))
		if err != nil {
			return errors.Wrapf(err, "read the row of edges of posting %d", p)
		}
		dp.lists, dp.block = nil, block
		dp.bytes[diskBlockEdges] = len(block)
		return nil
	}
	values, err := g.row(p, diskBlockEdges)
	if err != nil {
		return err
	}
	dp.lists = values
	bytes := 0
	for _, value := range values {
		if len(value) < 4 || len(value)%4 != 0 {
			return errors.Errorf("a list of edges of posting %d takes %d bytes", p, len(value))
		}
		bytes += len(value)
	}
	dp.bytes[diskBlockEdges] = bytes
	return nil
}

// joinEdges lays the lists of edges of a fetched posting out by member, in
// the order of its members. Every member has one list and only one.
func (dp *diskPosting) joinEdges(p uint32) error {
	n := len(dp.docIDs)
	if len(dp.lists) != n {
		// Which member is it about: the first with two lists, if any.
		seen := make(map[uint32][]byte, len(dp.lists))
		for _, value := range dp.lists {
			docID := binary.LittleEndian.Uint32(value)
			if other, twice := seen[docID]; twice {
				return errors.Errorf("posting %d has %d members and %d lists of edges: two of doc %d, % x and % x",
					p, n, len(dp.lists), docID, other, value)
			}
			seen[docID] = value
		}
		return errors.Errorf("posting %d has %d members and %d lists of edges", p, n, len(dp.lists))
	}
	dp.listOf = resized(dp.listOf, n)
	for i := range dp.listOf {
		dp.listOf[i] = -1
	}
	total := 0
	// The lists come in the order of the members until one is replaced: the
	// next member is looked at first.
	next := 0
	var byDoc map[uint64]int
	for at, value := range dp.lists {
		docID := uint64(binary.LittleEndian.Uint32(value))
		var member int
		if next < n && dp.docIDs[next] == docID {
			member = next
		} else {
			if byDoc == nil {
				byDoc = make(map[uint64]int, n)
				for i, id := range dp.docIDs {
					byDoc[id] = i
				}
			}
			found, ok := byDoc[docID]
			if !ok {
				return errors.Errorf("posting %d has a list of edges of doc %d, which is not a member", p, docID)
			}
			member = found
		}
		next = member + 1
		if dp.listOf[member] != -1 {
			return errors.Errorf("doc %d has two lists of edges in posting %d", docID, p)
		}
		dp.listOf[member] = int32(at)
		total += len(value)/4 - 1
	}
	dp.edgeStart, dp.edges = resized(dp.edgeStart, n+1), resized(dp.edges, total)
	dp.edgeStart[0] = 0
	at := 0
	for i := range n {
		value := dp.lists[dp.listOf[i]]
		for e := 4; e < len(value); e += 4 {
			dp.edges[at] = binary.LittleEndian.Uint32(value[e:])
			at++
		}
		dp.edgeStart[i+1] = uint32(at)
	}
	dp.lists = nil
	return nil
}

// fetch reads the two rows of every posting given, all at once, and leaves
// each posting ready to be visited.
func (g *growIndex) fetch(round *diskRound, picks []scored) error {
	if cap(round.postings) < len(picks) {
		round.postings = append(round.postings[:cap(round.postings)], make([]diskPosting, len(picks)-cap(round.postings))...)
	}
	round.postings = round.postings[:len(picks)]
	eg := enterrors.NewErrorGroupWrapper(g.logger)
	for i, pick := range picks {
		p, dp := uint32(pick.id), &round.postings[i]
		dp.bytes = [3]int{}
		eg.Go(func() error { return g.fetchMembers(p, dp) })
		eg.Go(func() error { return g.fetchLists(p, dp) })
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	for i, pick := range picks {
		dp := &round.postings[i]
		if err := g.joinLists(uint32(pick.id), dp); err != nil {
			return err
		}
		dp.post = posting{docIDs: dp.docIDs, codes: dp.codes, edges: dp.edges, edgeStart: dp.edgeStart, byDocID: true}
	}
	return nil
}

func (g *growIndex) searcher() *fetchedSearch {
	return &fetchedSearch{ix: g.ix, st: g.st, rounds: &g.rounds, fetch: g.fetch}
}

// search is the search of the index, with the postings fetched from the
// store.
func (g *growIndex) search(ctx context.Context, vector []float32, k int, stats *diskSearchStats,
) ([]uint64, []float32, error) {
	return g.searcher().run(ctx, vector, k, nil, stats)
}

// keep copies what the inverse edges of the insert need of a posting its
// search loaded.
func (g *growIndex) keep(p uint32, dp *diskPosting) {
	at := len(g.kept)
	if at < cap(g.kept) {
		g.kept = g.kept[:at+1]
	} else {
		g.kept = append(g.kept, growKept{})
	}
	kept := &g.kept[at]
	kept.docIDs = append(kept.docIDs[:0], dp.docIDs...)
	kept.edgeStart = append(kept.edgeStart[:0], dp.edgeStart...)
	kept.edges = append(kept.edges[:0], dp.edges...)
	g.keptAt[p] = at
}

// listOf returns the edges of doc u, a member of posting q, as the search of
// the insert left them, or read again from the store if it did not load q.
func (g *growIndex) listOf(q uint32, u uint64) ([]uint32, error) {
	at, found := g.keptAt[q]
	if !found {
		var dp diskPosting
		if err := g.fetchMembers(q, &dp); err != nil {
			return nil, err
		}
		if err := g.fetchLists(q, &dp); err != nil {
			return nil, err
		}
		g.stats.InverseListReads++
		if err := g.joinLists(q, &dp); err != nil {
			return nil, err
		}
		g.keep(q, &dp)
		at = g.keptAt[q]
	}
	kept := &g.kept[at]
	m := slices.Index(kept.docIDs, u)
	if m < 0 {
		return nil, errors.Errorf("doc %d is not in posting %d, where the map has it", u, q)
	}
	return kept.edges[kept.edgeStart[m]:kept.edgeStart[m+1]], nil
}

// counted reads the store's count of reads, zero without a counter.
func (g *growIndex) counted() (reads, bytes uint64, err error) {
	if g.storeReads == nil {
		return 0, 0, nil
	}
	return g.storeReads()
}

// vectorOfNeighbor returns the exact vector of a neighbor of the insert that
// runs: the one its rescore read, or read again.
func (g *growIndex) vectorOfNeighbor(docID uint64) ([]float32, error) {
	if vector, found := g.captured[docID]; found {
		return vector, nil
	}
	_, vector, err := g.exact(docID, nil, nil)
	if err != nil {
		return nil, err
	}
	g.stats.InverseVectors.add(1, 4*g.meta.Dims)
	g.captured[docID] = vector
	return vector, nil
}

// insert adds one vector by the incremental path of the streaming index
// (streamIndex.insert), with inverse edges and a split that touches no edge,
// reading and writing the store. The vector must be normalized as the index
// stores it. The search parameters of the edge search are the index's own.
func (g *growIndex) insert(ctx context.Context, docID uint64, vector []float32) error {
	if g.slotOf != nil {
		return errors.New("the index was laid out again to be searched: it does not grow")
	}
	if docID >= uint64(len(g.live)) {
		return errors.Errorf("doc id %d is past the %d keys", docID, len(g.live))
	}
	if g.live[docID] {
		return errors.Errorf("doc %d is in the index already", docID)
	}
	if len(vector) != g.meta.Dims {
		return errors.Errorf("a vector of %d dimensions, the index holds %d", len(vector), g.meta.Dims)
	}
	st := g.st

	clock := time.Now()
	entry, err := st.centroids.nearest(ctx, vector, max(int(g.ix.entryPostings.Load()), 1))
	if err != nil {
		return err
	}
	if len(entry) == 0 {
		return errors.New("the centroid graph found no centroid")
	}
	p := uint32(entry[0])
	for _, e := range entry {
		if e >= uint64(len(g.retired)) || g.retired[e] {
			return errors.Errorf("the centroid graph returned posting %d, which is not live", e)
		}
	}
	g.stats.AssignTime += time.Since(clock)

	// The search for its edges, which leaves behind the edges of what it
	// loaded and the vectors it rescored.
	clock = time.Now()
	g.kept = g.kept[:0]
	clear(g.keptAt)
	clear(g.captured)
	reads, bytes, err := g.counted()
	if err != nil {
		return err
	}
	vectorsBefore := g.vectorReads.Load()
	search := g.searcher()
	search.loaded = g.keep
	var searched diskSearchStats
	if g.work != nil {
		*g.work = SearchStats{}
		searched.Work = g.work
	}
	g.capturing.Store(true)
	neighbors, dists, err := search.run(ctx, vector, g.meta.Neighbors, entry, &searched)
	g.capturing.Store(false)
	if err != nil {
		return errors.Wrapf(err, "search the neighbors of doc %d", docID)
	}
	g.stats.SearchLoads += searched.Postings
	g.stats.SearchRounds += searched.Rounds
	g.stats.SearchRows.add(searched.Blocks, searched.BlockBytes)
	g.stats.SearchVectors.add(int(g.vectorReads.Load()-vectorsBefore), int(g.vectorReads.Load()-vectorsBefore)*4*g.meta.Dims)
	readsAfter, bytesAfter, err := g.counted()
	if err != nil {
		return err
	}
	g.stats.StoreSearch.add(int(readsAfter-reads), int(bytesAfter-bytes))
	reads, bytes = readsAfter, bytesAfter
	g.stats.SearchTime += time.Since(clock)

	// The vector, in its posting, with its edges: of the neighbors, those
	// outside its posting. The distance of every edge is the exact one from
	// the vector to its endpoint, which is what a later inverse edge
	// compares against.
	clock = time.Now()
	routingCode := st.quantizer.Encode(vector)
	scanCode := routingCode
	if st.scanQuantizer != nil {
		scanCode = st.scanQuantizer.Encode(vector)
	}
	edges := make([]uint32, 0, len(neighbors))
	edgeDists := make([]float32, 0, len(neighbors))
	for _, u := range neighbors {
		if st.side.postingOf[u] == p {
			continue
		}
		to, err := g.vectorOfNeighbor(u)
		if err != nil {
			return err
		}
		dist, err := g.provider.SingleDist(vector, to)
		if err != nil {
			return err
		}
		edges, edgeDists = append(edges, uint32(u)), append(edgeDists, dist)
	}
	member, list := growMemberValue(docID, scanCode), growEdgesValue(docID, edges)
	if err := g.postings.SetAdd(diskKey(p, diskBlockMembers), [][]byte{member}); err != nil {
		return errors.Wrapf(err, "write doc %d to posting %d", docID, p)
	}
	if err := g.posts.Put(growDocKey(docID), growUint32Value(p)); err != nil {
		return errors.Wrap(err, "write the posting map")
	}
	routingValue, distsValue := growCodeValue(routingCode), growDistsValue(edgeDists)
	if err := g.routing.Put(growDocKey(docID), routingValue); err != nil {
		return errors.Wrap(err, "write a routing code")
	}
	g.touched = g.touched[:0]
	if g.edges != nil {
		// Its list and its distances wait with the other changes of its
		// posting.
		g.setPending(p, docID, edges, edgeDists)
		list, distsValue = nil, nil
	} else {
		if err := g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{list}); err != nil {
			return errors.Wrapf(err, "write the edges of doc %d", docID)
		}
		if err := g.dists.Put(growDocKey(docID), distsValue); err != nil {
			return errors.Wrap(err, "write the distances of the edges")
		}
	}
	raw := make([]byte, 4*len(vector))
	for i, x := range vector {
		putFloat32(raw[4*i:], x)
	}
	if _, err := g.vectors.WriteAt(raw, int64(docID)*int64(len(raw))); err != nil {
		return errors.Wrapf(err, "write the vector of doc %d", docID)
	}
	g.stats.MemberRow.add(1, len(member))
	g.stats.MemberMap.add(1, 4)
	g.stats.MemberRouting.add(1, len(routingValue))
	g.stats.MemberVector.add(1, len(raw))
	if g.edges == nil {
		g.stats.OwnEdges.add(1, len(list))
		g.stats.OwnDists.add(1, len(distsValue))
	}
	words := st.side.codeWords
	copy(st.side.codes[int(docID)*words:(int(docID)+1)*words], routingCode)
	st.side.postingOf[docID] = p
	g.live[docID] = true
	g.size[p]++
	g.meta.Live++
	g.stats.Inserted++
	g.lastPosting, g.lastEdges = p, edges
	g.stats.WriteTime += time.Since(clock)

	clock = time.Now()
	if err := g.enterNeighbors(docID, vector, p, neighbors, dists); err != nil {
		return err
	}
	readsAfter, bytesAfter, err = g.counted()
	if err != nil {
		return err
	}
	g.stats.StoreInverse.add(int(readsAfter-reads), int(bytesAfter-bytes))
	reads, bytes = readsAfter, bytesAfter
	g.stats.InverseTime += time.Since(clock)

	if int(g.size[p]) > g.meta.Threshold {
		clock = time.Now()
		if err := g.split(ctx, p); err != nil {
			return errors.Wrapf(err, "split posting %d", p)
		}
		readsAfter, bytesAfter, err = g.counted()
		if err != nil {
			return err
		}
		g.stats.StoreSplit.add(int(readsAfter-reads), int(bytesAfter-bytes))
		reads, bytes = readsAfter, bytesAfter
		g.stats.SplitTime += time.Since(clock)
	}

	if g.edges != nil {
		// The rows of edges of the postings this insert changed, once they
		// have their share of changes. A posting that split has none left.
		clock = time.Now()
		for _, q := range g.touched {
			if pend := g.pending[q]; pend != nil && pend.changes >= g.meta.Consolidate {
				if err := g.writeRow(q, &g.stats.RowWritesFull); err != nil {
					return err
				}
			}
		}
		readsAfter, bytesAfter, err = g.counted()
		if err != nil {
			return err
		}
		g.stats.StoreRows.add(int(readsAfter-reads), int(bytesAfter-bytes))
		g.stats.RowTime += time.Since(clock)
	}

	if g.sinceCycle++; g.sinceCycle >= growCycleEvery {
		clock = time.Now()
		g.sinceCycle = 0
		g.cycle()
		g.stats.CycleTime += time.Since(clock)
	}
	return nil
}

// enterNeighbors gives the new vector, in posting p, a place in the edges of
// each of its neighbors outside p whose worst edge it beats, as the
// streaming index does (streamIndex.enterNeighbors): it takes the place its
// distance gives it, and the worst edge goes. Where the streaming index
// computes the distances of the neighbor's edges again, this reads the ones
// that were stored with them. dists are the distances of the search from
// the vector to its neighbors.
func (g *growIndex) enterNeighbors(docID uint64, vector []float32, p uint32, neighbors []uint64, dists []float32) error {
	var written []uint32
	for i, u := range neighbors {
		q := g.st.side.postingOf[u]
		if q == p {
			continue
		}
		edges, err := g.listOf(q, u)
		if err != nil {
			return err
		}
		if len(edges) == 0 {
			continue
		}
		if slices.Contains(edges, uint32(docID)) {
			g.stats.InverseAlready++
			continue
		}
		g.stats.InverseChecks++
		edgeDists, read, err := g.distsOf(q, u)
		if err != nil {
			return err
		}
		if read > 0 || g.edges == nil {
			g.stats.InverseDists.add(1, read)
		}
		if len(edgeDists) != len(edges) {
			return errors.Errorf("doc %d has %d edges and %d distances", u, len(edges), len(edgeDists))
		}
		if dists[i] >= edgeDists[len(edges)-1] {
			continue
		}
		at := len(edges) - 1
		for at > 0 && edgeDists[at-1] > dists[i] {
			at--
		}
		// The distance that is stored for the new edge is the one the
		// streaming index would compute for it later: from the neighbor to
		// the vector.
		from, err := g.vectorOfNeighbor(u)
		if err != nil {
			return err
		}
		exact, err := g.provider.SingleDist(from, vector)
		if err != nil {
			return err
		}
		updated := make([]uint32, 0, len(edges))
		updated = append(append(append(updated, edges[:at]...), uint32(docID)), edges[at:len(edges)-1]...)
		updatedDists := make([]float32, 0, len(edges))
		updatedDists = append(append(append(updatedDists, edgeDists[:at]...), exact), edgeDists[at:len(edges)-1]...)

		// The list of the neighbor changes without its row being read or
		// written again: the new list goes in, and a tombstone of the old one.
		if g.edges != nil {
			// With consolidated edges the new list waits in memory, with its
			// distances, until the row of its posting is written.
			g.setPending(q, u, updated, updatedDists)
		} else {
			old, list, distsValue := growEdgesValue(u, edges), growEdgesValue(u, updated), growDistsValue(updatedDists)
			if err := g.postings.SetDeleteSingle(diskKey(q, diskBlockEdges), old); err != nil {
				return errors.Wrapf(err, "take the old edges of doc %d out", u)
			}
			if err := g.postings.SetAdd(diskKey(q, diskBlockEdges), [][]byte{list}); err != nil {
				return errors.Wrapf(err, "write the edges of doc %d", u)
			}
			if err := g.dists.Put(growDocKey(u), distsValue); err != nil {
				return errors.Wrapf(err, "write the distances of the edges of doc %d", u)
			}
			g.stats.InverseTombs.add(1, len(old))
			g.stats.InverseLists.add(1, len(list))
			g.stats.InverseDistsW.add(1, len(distsValue))
		}
		// What the insert kept of the posting follows, should the same list
		// be looked at again.
		copy(edges, updated)
		g.stats.InverseEdges++
		if !slices.Contains(written, q) {
			written = append(written, q)
		}
	}
	g.stats.InversePostings += len(written)
	return nil
}

// split parts posting p in two, as the streaming index does
// (streamIndex.split): the 2-means of the batch partition over the exact
// vectors of its members, two new postings with their centroids, and p
// retired. Its two rows are read whole and written again as the rows of the
// two halves. No edge changes: only the posting map does.
func (g *growIndex) split(ctx context.Context, p uint32) error {
	st := g.st
	var dp diskPosting
	if err := g.fetchMembers(p, &dp); err != nil {
		return err
	}
	if err := g.fetchLists(p, &dp); err != nil {
		return err
	}
	g.stats.SplitRows.add(2, dp.bytes[diskBlockMembers]+dp.bytes[diskBlockEdges])
	if err := g.joinLists(p, &dp); err != nil {
		return err
	}
	n := len(dp.docIDs)
	vectors := make([][]float32, n)
	members := make([]uint32, n)
	var raw []byte
	for i, docID := range dp.docIDs {
		var err error
		if raw, vectors[i], err = g.exact(docID, raw, nil); err != nil {
			return err
		}
		members[i] = uint32(i)
	}
	g.stats.SplitVectors.add(n, n*4*g.meta.Dims)

	failed := func() error {
		// A 2-means that leaves one side empty is counted, not fatal: the
		// posting tries again on its next insert.
		g.stats.SplitsFailed++
		g.meta.SplitsFailed++
		return nil
	}
	left, right, _, err := bisect(vectors, st.dims, cluster{members: members, leaves: 2, path: 1},
		g.meta.Seed^uint64(g.meta.Splits+g.meta.SplitsFailed))
	if err != nil {
		return failed()
	}
	means := [2][]float32{mean(vectors, left.members, g.sums), mean(vectors, right.members, g.sums)}
	sideOf := make([]int, n)
	sizes := [2]int{}
	for i, vec := range vectors {
		da, err := g.l2.SingleDist(vec, means[0])
		if err != nil {
			return err
		}
		db, err := g.l2.SingleDist(vec, means[1])
		if err != nil {
			return err
		}
		if db < da {
			sideOf[i] = 1
		}
		sizes[sideOf[i]]++
	}
	if sizes[0] == 0 || sizes[1] == 0 {
		return failed()
	}

	first := uint32(len(g.retired))
	words := g.meta.ScanCodeWords
	var rows [2][2][][]byte
	var halfIDs [2][]uint64
	var halfLists [2][][]uint32
	for i, docID := range dp.docIDs {
		c := sideOf[i]
		rows[c][0] = append(rows[c][0], growMemberValue(docID, dp.codes[i*words:(i+1)*words]))
		if g.edges != nil {
			halfIDs[c], halfLists[c] = append(halfIDs[c], docID), append(halfLists[c], slices.Clone(dp.edgesOfMember(i)))
			continue
		}
		rows[c][1] = append(rows[c][1], growEdgesValue(docID, dp.edgesOfMember(i)))
	}
	if g.edges != nil {
		// The rows of edges of the halves are written whole, with what
		// waited of the posting in them, and the distances that waited go
		// to their bucket. The row of the posting itself is deleted.
		for c := range uint32(2) {
			block, err := growEdgesBlock(halfIDs[c], halfLists[c])
			if err != nil {
				return err
			}
			if err := g.edges.Put(growPostingKey(first+c), block); err != nil {
				return errors.Wrapf(err, "write the row of edges of posting %d", first+c)
			}
			g.stats.RowWrites.add(1, len(block))
			g.stats.RowWritesSplit++
		}
		if err := g.dropPending(p, true); err != nil {
			return err
		}
		if err := g.edges.Delete(growPostingKey(p)); err != nil {
			return errors.Wrapf(err, "delete the row of edges of posting %d", p)
		}
		g.stats.RowDeletes++
	}
	for c := range uint32(2) {
		for block, values := range rows[c] {
			if len(values) == 0 {
				continue
			}
			if err := g.postings.SetAdd(diskKey(first+c, byte(block)), values); err != nil {
				return errors.Wrapf(err, "write row %d of posting %d", block, first+c)
			}
			bytes := 0
			for _, value := range values {
				bytes += len(value)
			}
			g.stats.SplitRowsW.add(len(values), bytes)
		}
	}
	for i, docID := range dp.docIDs {
		child := first + uint32(sideOf[i])
		if err := g.posts.Put(growDocKey(docID), growUint32Value(child)); err != nil {
			return errors.Wrap(err, "write the posting map")
		}
		st.side.postingOf[docID] = child
	}
	g.stats.SplitMap.add(n, 4*n)

	// The centroids of the halves, on disk and in the graph, and the old one
	// out of it.
	rawMeans := make([]byte, 2*4*st.dims)
	for c, m := range means {
		for i, x := range m {
			putFloat32(rawMeans[4*(c*st.dims+i):], x)
		}
	}
	if _, err := g.centroids.WriteAt(rawMeans, int64(first)*int64(4*st.dims)); err != nil {
		return errors.Wrap(err, "write the centroids of the halves")
	}
	g.stats.SplitCentroid.add(2, len(rawMeans))
	centroids := st.centroids
	centroids.vectors = append(centroids.vectors, means[0], means[1])
	for c := range 2 {
		if err := centroids.graph.Add(ctx, uint64(first)+uint64(c), means[c]); err != nil {
			return errors.Wrap(err, "add a centroid")
		}
	}
	if err := centroids.graph.Delete(uint64(p)); err != nil {
		return errors.Wrap(err, "delete the centroid")
	}

	g.retiredMu.Lock()
	g.retired = append(g.retired, false, false)
	g.retired[p] = true
	g.retiredMu.Unlock()
	g.size = append(g.size, uint32(sizes[0]), uint32(sizes[1]))
	g.size[p] = 0
	g.meta.Postings += 2
	g.meta.Splits++
	g.stats.Splits++
	return nil
}

// edgesOfMember returns the edges of member i of a posting whose edges were
// joined.
func (dp *diskPosting) edgesOfMember(i int) []uint32 {
	return dp.edges[dp.edgeStart[i]:dp.edgeStart[i+1]]
}

// joinLists lays the edges of a fetched posting out by member, in the order
// of its members: from its lists, one value each, or, with consolidated
// edges, from its row with what waits in memory over it.
func (g *growIndex) joinLists(p uint32, dp *diskPosting) error {
	if g.edges == nil {
		return dp.joinEdges(p)
	}
	row := &dp.row
	if dp.block == nil {
		row.ids, row.start, row.edges = row.ids[:0], append(row.start[:0], 0), row.edges[:0]
	} else if err := row.decode(dp.block); err != nil {
		return errors.Wrapf(err, "posting %d", p)
	}
	pend := g.pending[p]
	n := len(dp.docIDs)
	dp.edgeStart, dp.edges = resized(dp.edgeStart, n+1), dp.edges[:0]
	dp.edgeStart[0] = 0
	var byDoc map[uint32]int
	for i, docID := range dp.docIDs {
		var list []uint32
		found := false
		if pend != nil {
			list, found = pend.lists[docID]
		}
		if !found {
			// The row is in the order of the members: the same place is
			// looked at first.
			at := -1
			if i < len(row.ids) && uint64(row.ids[i]) == docID {
				at = i
			} else {
				if byDoc == nil {
					byDoc = make(map[uint32]int, len(row.ids))
					for j, id := range row.ids {
						byDoc[id] = j
					}
				}
				if j, ok := byDoc[uint32(docID)]; ok {
					at = j
				}
			}
			if at < 0 {
				return errors.Errorf("doc %d, a member of posting %d, has no list of edges: neither in its row nor waiting", docID, p)
			}
			list = row.edges[row.start[at]:row.start[at+1]]
		}
		dp.edges = append(dp.edges, list...)
		dp.edgeStart[i+1] = uint32(len(dp.edges))
	}
	return nil
}

// setPending leaves the list of a member of posting p, and the distances of
// its edges, waiting in memory: a change of the posting.
func (g *growIndex) setPending(p uint32, docID uint64, list []uint32, dists []float32) {
	pend := g.pending[p]
	if pend == nil {
		pend = &growPending{lists: make(map[uint64][]uint32), dists: make(map[uint64][]float32)}
		g.pending[p] = pend
	}
	if old, waiting := pend.lists[docID]; waiting {
		pend.bytes -= int64(4*len(old) + 4*len(pend.dists[docID]))
		g.pendingBytes -= int64(4*len(old) + 4*len(pend.dists[docID]))
	} else {
		pend.order = append(pend.order, docID)
		g.pendingMembers++
	}
	pend.lists[docID], pend.dists[docID] = slices.Clone(list), slices.Clone(dists)
	pend.bytes += int64(4*len(list) + 4*len(dists))
	g.pendingBytes += int64(4*len(list) + 4*len(dists))
	pend.changes++
	if g.pendingBytes > g.stats.PendingPeakBytes {
		g.stats.PendingPeakBytes, g.stats.PendingPeak, g.stats.PendingPeakPosts = g.pendingBytes, g.pendingMembers, len(g.pending)
	}
	if !slices.Contains(g.touched, p) {
		g.touched = append(g.touched, p)
	}
}

// distsOf returns the distances of the edges of a member of posting p: the
// ones that wait with its list, or the ones in their bucket. read is the
// bytes it read from the bucket, 0 when they were waiting.
func (g *growIndex) distsOf(p uint32, docID uint64) (dists []float32, read int, err error) {
	if pend := g.pending[p]; pend != nil {
		if dists, waiting := pend.dists[docID]; waiting {
			return dists, 0, nil
		}
	}
	stored, err := g.dists.Get(growDocKey(docID))
	if err != nil {
		return nil, 0, errors.Wrapf(err, "read the distances of the edges of doc %d", docID)
	}
	if len(stored)%4 != 0 {
		return nil, 0, errors.Errorf("doc %d has %d bytes of distances", docID, len(stored))
	}
	dists = make([]float32, len(stored)/4)
	for e := range dists {
		dists[e] = getFloat32(stored[4*e:])
	}
	return dists, len(stored), nil
}

// dropPending forgets what waits of posting p. With its distances written
// first, which is what a split and the writing of a row want: the lists then
// are in a row on disk.
func (g *growIndex) dropPending(p uint32, writeDists bool) error {
	pend := g.pending[p]
	if pend == nil {
		return nil
	}
	if writeDists {
		for _, docID := range pend.order {
			value := growDistsValue(pend.dists[docID])
			if err := g.dists.Put(growDocKey(docID), value); err != nil {
				return errors.Wrapf(err, "write the distances of the edges of doc %d", docID)
			}
			g.stats.RowDists.add(1, len(value))
		}
	}
	g.pendingMembers -= int64(len(pend.order))
	g.pendingBytes -= pend.bytes
	delete(g.pending, p)
	return nil
}

// writeRow writes the row of edges of posting p whole, as a new version: the
// one on disk with what waits in memory over it. The members it has no list
// of yet, the ones that came since it was written, go after the others, in
// the order they came. why counts what had it written.
func (g *growIndex) writeRow(p uint32, why *int) error {
	pend := g.pending[p]
	if pend == nil {
		return nil
	}
	old, err := g.edges.Get(growPostingKey(p))
	if err != nil {
		return errors.Wrapf(err, "read the row of edges of posting %d", p)
	}
	g.stats.RowReads.add(1, len(old))
	var row growBlock
	if old != nil {
		if err := row.decode(old); err != nil {
			return errors.Wrapf(err, "posting %d", p)
		}
	}
	ids := make([]uint64, 0, len(row.ids)+len(pend.order))
	lists := make([][]uint32, 0, len(row.ids)+len(pend.order))
	inRow := make(map[uint64]bool, len(row.ids))
	for i, id := range row.ids {
		docID := uint64(id)
		inRow[docID] = true
		list, waiting := pend.lists[docID]
		if !waiting {
			list = row.edges[row.start[i]:row.start[i+1]]
		}
		ids, lists = append(ids, docID), append(lists, list)
	}
	for _, docID := range pend.order {
		if !inRow[docID] {
			ids, lists = append(ids, docID), append(lists, pend.lists[docID])
		}
	}
	block, err := growEdgesBlock(ids, lists)
	if err != nil {
		return err
	}
	if err := g.edges.Put(growPostingKey(p), block); err != nil {
		return errors.Wrapf(err, "write the row of edges of posting %d", p)
	}
	g.stats.RowWrites.add(1, len(block))
	*why++
	return g.dropPending(p, true)
}

// flushPending writes the row of edges of every posting that has changes
// waiting. After it nothing waits: what the index is, is in its store.
func (g *growIndex) flushPending() error {
	if g.edges == nil {
		return nil
	}
	postings := make([]uint32, 0, len(g.pending))
	for p := range g.pending {
		postings = append(postings, p)
	}
	slices.Sort(postings)
	for _, p := range postings {
		if err := g.writeRow(p, &g.stats.RowWritesClose); err != nil {
			return err
		}
	}
	return nil
}

// cleanCentroidGraph removes the deleted centroids from the graph for good.
func (g *growIndex) cleanCentroidGraph() error {
	return g.st.centroids.graph.CleanUpTombstonedNodes(func() bool { return false })
}

// checkCoverage checks the invariant of the inserts against what is on disk,
// as the streaming index checks it against its memory
// (streamIndex.checkCoverage): every live vector is in exactly one posting,
// the one the persisted map gives it, with a routing code; no live posting
// holds anything else; every member has one list of edges, which lead to
// live vectors outside its own posting, with a distance each; and the
// centroid graph holds the live postings and none of the retired ones. It
// reads every row of the store.
func (g *growIndex) checkCoverage() error {
	st := g.st
	keys := len(g.live)
	mapped := make([]uint32, keys)
	for i := range mapped {
		mapped[i] = unplaced
	}
	inMap := 0
	err := growScan(g.posts, func(key, value []byte) error {
		docID := binary.BigEndian.Uint32(key)
		if int(docID) >= keys {
			return errors.Errorf("the map on disk holds doc %d, past the %d keys", docID, keys)
		}
		mapped[docID] = binary.LittleEndian.Uint32(value)
		inMap++
		return nil
	})
	if err != nil {
		return err
	}
	if inMap != g.meta.Live {
		return errors.Errorf("the map on disk holds %d docs, %d are live", inMap, g.meta.Live)
	}
	for docID, p := range mapped {
		if p != st.side.postingOf[docID] {
			return errors.Errorf("the map on disk puts doc %d in posting %d, the one in memory in %d", docID, p, st.side.postingOf[docID])
		}
		if g.live[docID] != (p != unplaced) {
			return errors.Errorf("doc %d is live: %v, and the map on disk puts it in %d", docID, g.live[docID], p)
		}
	}
	coded := 0
	words := st.side.codeWords
	err = growScan(g.routing, func(key, value []byte) error {
		docID := binary.BigEndian.Uint32(key)
		if int(docID) >= keys || !g.live[docID] {
			return errors.Errorf("a routing code on disk for doc %d, which is not live", docID)
		}
		for i := range words {
			if binary.LittleEndian.Uint64(value[8*i:]) != st.side.codes[int(docID)*words+i] {
				return errors.Errorf("doc %d has another routing code on disk than in memory", docID)
			}
		}
		coded++
		return nil
	})
	if err != nil {
		return err
	}
	if coded != g.meta.Live {
		return errors.Errorf("%d routing codes on disk for %d live docs", coded, g.meta.Live)
	}

	if len(g.retired) != g.meta.Postings || len(g.size) != g.meta.Postings || len(st.centroids.vectors) != g.meta.Postings {
		return errors.Errorf("%d postings, %d retired flags, %d sizes and %d centroids",
			g.meta.Postings, len(g.retired), len(g.size), len(st.centroids.vectors))
	}
	count := make([]uint8, keys)
	var dp diskPosting
	for p := range uint32(g.meta.Postings) {
		if g.retired[p] {
			if g.size[p] != 0 {
				return errors.Errorf("retired posting %d counts %d vectors", p, g.size[p])
			}
			if st.centroids.graph.ContainsDoc(uint64(p)) {
				return errors.Errorf("retired posting %d is still in the centroid graph", p)
			}
			if g.edges != nil {
				// Its row of edges went with it, and nothing of it waits.
				row, err := g.edges.Get(growPostingKey(p))
				if err != nil {
					return errors.Wrapf(err, "read the row of edges of posting %d", p)
				}
				if row != nil || g.pending[p] != nil {
					return errors.Errorf("retired posting %d still has a row of edges, or changes waiting", p)
				}
			}
			continue
		}
		if !st.centroids.graph.ContainsDoc(uint64(p)) {
			return errors.Errorf("live posting %d is not in the centroid graph", p)
		}
		if err := g.fetchMembers(p, &dp); err != nil {
			return err
		}
		if err := g.fetchLists(p, &dp); err != nil {
			return err
		}
		if err := g.joinLists(p, &dp); err != nil {
			return err
		}
		if len(dp.docIDs) == 0 {
			return errors.Errorf("live posting %d is empty", p)
		}
		if g.edges != nil {
			// Its row of edges and what waits hold a list for its members
			// and for nobody else: joinLists found one for each.
			lists := len(dp.row.ids)
			if pend := g.pending[p]; pend != nil {
				for _, docID := range pend.order {
					if !slices.Contains(dp.row.ids, uint32(docID)) {
						lists++
					}
				}
			}
			if lists != len(dp.docIDs) {
				return errors.Errorf("posting %d has %d members and %d lists of edges, in its row and waiting", p, len(dp.docIDs), lists)
			}
		}
		for i, docID := range dp.docIDs {
			if docID >= uint64(keys) || !g.live[docID] {
				return errors.Errorf("posting %d holds doc %d, which is not live", p, docID)
			}
			if mapped[docID] != p {
				return errors.Errorf("posting %d holds doc %d, which the map puts in %d", p, docID, mapped[docID])
			}
			if count[docID]++; count[docID] > 1 {
				return errors.Errorf("doc %d is in two postings", docID)
			}
			edges := dp.edgesOfMember(i)
			for _, endpoint := range edges {
				if int(endpoint) >= keys || !g.live[endpoint] {
					return errors.Errorf("doc %d has an edge to doc %d, which is not live", docID, endpoint)
				}
				if mapped[endpoint] == p {
					return errors.Errorf("doc %d has an edge into its own posting %d, to doc %d", docID, p, endpoint)
				}
			}
			stored, _, err := g.distsOf(p, docID)
			if err != nil {
				return err
			}
			if len(stored) != len(edges) {
				return errors.Errorf("doc %d has %d edges and %d bytes of distances", docID, len(edges), 4*len(stored))
			}
		}
	}
	for docID, live := range g.live {
		if live && count[docID] != 1 {
			return errors.Errorf("live doc %d is in %d postings", docID, count[docID])
		}
	}
	return nil
}

// sameAs tells the first difference between the index on disk and a
// streaming index that grew by the same inserts: the postings with their
// members in order, their codes and their edges, read from the store, the
// posting map and the routing codes. Nil when there is none.
func (g *growIndex) sameAs(s *streamIndex) error {
	lists := s.st.postings.lists
	if len(lists) != g.meta.Postings {
		return errors.Errorf("%d postings on disk, %d in memory", g.meta.Postings, len(lists))
	}
	if !slices.Equal(g.live, s.live) {
		return errors.New("the live docs differ")
	}
	if !slices.Equal(g.st.side.postingOf, s.st.side.postingOf) {
		return errors.New("the posting maps differ")
	}
	if !slices.Equal(g.st.side.codes, s.st.side.codes) {
		return errors.New("the routing codes differ")
	}
	if !slices.Equal(g.retired, s.retired) {
		return errors.New("the retired postings differ")
	}
	var dp diskPosting
	for p := range uint32(len(lists)) {
		l := &lists[p]
		if g.retired[p] {
			continue
		}
		if err := g.fetchMembers(p, &dp); err != nil {
			return err
		}
		if err := g.fetchLists(p, &dp); err != nil {
			return err
		}
		if err := g.joinLists(p, &dp); err != nil {
			return err
		}
		switch {
		case !slices.Equal(dp.docIDs, l.docIDs):
			return errors.Errorf("posting %d holds other members on disk, or in another order: %d and %d of them",
				p, len(dp.docIDs), len(l.docIDs))
		case !slices.Equal(dp.codes, l.codes):
			return errors.Errorf("posting %d holds other codes on disk", p)
		}
		for i, docID := range dp.docIDs {
			if !slices.Equal(dp.edgesOfMember(i), l.edgesOf(i)) {
				return errors.Errorf("doc %d, in posting %d, has the edges %v on disk and %v in memory",
					docID, p, dp.edgesOfMember(i), l.edgesOf(i))
			}
		}
	}
	return nil
}

// reorganizeGrowIndex writes to dir, which must exist and be empty, the index
// src holds, laid out again as a reorganization in the background would
// leave it, without the compaction of the store: the control of v5.0
// (bench-report.md, "control de reorganización"). Every row of every live
// posting is read from src and added to a new bucket that is flushed once,
// so that it ends as one segment, with nothing that was replaced in it. The
// format of the rows is the same. The other buckets are written again entry
// by entry, and the centroids and their graph are copied.
//
// With orderVectors the exact vectors are written again too, those of every
// posting in a row, in the order of its members, and the index gets the
// table that tells where the vector of every doc id is. Without it the
// vectors file is the one of src, linked.
func reorganizeGrowIndex(ctx context.Context, logger logrus.FieldLogger, src *growIndex, dir string, orderVectors bool,
) (err error) {
	if entries, err := os.ReadDir(dir); err != nil {
		return err
	} else if len(entries) > 0 {
		return errors.Errorf("%s is not empty", dir)
	}
	if src.slotOf != nil {
		return errors.New("the index was laid out again already")
	}
	if src.edges != nil {
		return errors.New("the index has its edges consolidated: the control lays out one with a value per list")
	}
	if err := src.sync(); err != nil {
		return err
	}
	path := func(name string) string { return filepath.Join(dir, name) }
	for _, name := range []string{diskMeanFile, diskCentroidFile} {
		raw, err := os.ReadFile(filepath.Join(src.dir, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(path(name), raw, 0o644); err != nil {
			return err
		}
	}
	if err := os.CopyFS(path(diskGraphDir), os.DirFS(filepath.Join(src.dir, diskGraphDir))); err != nil {
		return errors.Wrap(err, "copy the centroid graph")
	}

	dst := &growIndex{dir: dir, logger: logger, provider: src.provider, meta: src.meta}
	if err := dst.openStore(ctx, growOpen{}); err != nil {
		return err
	}
	defer func() {
		if shut := dst.lsm.Shutdown(ctx); err == nil {
			err = errors.Wrap(shut, "close the store")
		}
	}()
	// Nothing is flushed until everything is written: the store only
	// flushes when its cycles are called, and nothing calls them here.
	for _, buckets := range [][2]*lsmkv.Bucket{{src.posts, dst.posts}, {src.routing, dst.routing}, {src.dists, dst.dists}} {
		err := growScan(buckets[0], func(key, value []byte) error {
			return buckets[1].Put(slices.Clone(key), slices.Clone(value))
		})
		if err != nil {
			return errors.Wrap(err, "write a bucket again")
		}
	}

	dims, words := src.meta.Dims, src.meta.ScanCodeWords
	var slotOf []uint32
	var vectors *os.File
	if orderVectors {
		slotOf = make([]uint32, src.meta.Keys)
		for i := range slotOf {
			slotOf[i] = diskNoSlot
		}
		if vectors, err = os.OpenFile(path(diskVectorFile), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
			return err
		}
		defer func() {
			if closed := vectors.Close(); err == nil {
				err = closed
			}
		}()
	}
	var dp diskPosting
	var raw, batch []byte
	slot := uint32(0)
	for p := range uint32(src.meta.Postings) {
		if src.retired[p] {
			continue
		}
		if err := src.fetchMembers(p, &dp); err != nil {
			return err
		}
		if err := src.fetchLists(p, &dp); err != nil {
			return err
		}
		if err := src.joinLists(p, &dp); err != nil {
			return err
		}
		n := len(dp.docIDs)
		members, lists := make([][]byte, n), make([][]byte, n)
		batch = batch[:0]
		first := slot
		for i, docID := range dp.docIDs {
			members[i] = growMemberValue(docID, dp.codes[i*words:(i+1)*words])
			lists[i] = growEdgesValue(docID, dp.edgesOfMember(i))
			if orderVectors {
				if raw, _, err = src.exact(docID, raw, nil); err != nil {
					return err
				}
				batch = append(batch, raw...)
				slotOf[docID] = slot
				slot++
			}
		}
		if err := dst.postings.SetAdd(diskKey(p, diskBlockMembers), members); err != nil {
			return errors.Wrapf(err, "write the members of posting %d", p)
		}
		if err := dst.postings.SetAdd(diskKey(p, diskBlockEdges), lists); err != nil {
			return errors.Wrapf(err, "write the edges of posting %d", p)
		}
		if orderVectors {
			// The vectors of a posting, in a row.
			if _, err := vectors.WriteAt(batch, int64(first)*int64(4*dims)); err != nil {
				return errors.Wrapf(err, "write the vectors of posting %d", p)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if orderVectors {
		if err := writeArray(path(growSlotsFile), 4, binary.LittleEndian.PutUint32, slotOf); err != nil {
			return errors.Wrap(err, "write where the vectors are")
		}
	} else if err := os.Link(filepath.Join(src.dir, diskVectorFile), path(diskVectorFile)); err != nil {
		return errors.Wrap(err, "link the vectors")
	}
	if err := dst.flushAll(); err != nil {
		return err
	}
	return dst.writeMeta()
}

// sizeOnDisk returns the bytes of the files under a directory.
func sizeOnDisk(dir string) (int64, error) {
	total := int64(0)
	err := filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
