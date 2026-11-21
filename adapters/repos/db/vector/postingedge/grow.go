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
//	vectors.f32    the exact vector of doc id d at position d; or, in an
//	               index whose vectors have slots (growMeta.VectorGen),
//	               vectors-<gen>.f32, the vectors one after another in the
//	               order they were laid out or came in, and slots-<gen>.u32,
//	               where the vector of every doc id is (grow_layout.go)
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
//	  deleted   strategy Replace, only in an index opened with deletes: doc
//	            id -> a mark. A deleted vector keeps its place in its
//	            posting, in the map and among the routing codes.
//
// The edges are reached through growEdges (grow_edges.go): the row of edges
// above is the one of v5.0, and grow_rows.go has the other, one value with
// the lists of all the members of a posting, with one bucket more where it is
// asked for:
//
//	  pending   strategy Replace: doc id -> the list of edges of a member that
//	            changed since the row of its posting was written, with its
//	            distances. Read when the index opens, and never by a search.
//
// In memory: the routing codes, the posting map, the marks of the deleted
// vectors, the centroids and their graph. One writer: inserts run one after
// another, and no search runs next to the writes of an insert, to a delete
// or to whatever gives the index more room. Whoever owns the index sees to
// it: the tests by running one thing at a time, and the index a shard opens
// with a lock (persistent.go). The search of an insert for its edges (plan)
// writes nothing, and runs next to other searches.
//
// A shard gives the index its store, where the buckets carry the id of the
// index in their names (growOpen.store). The experiments give it none, and
// it opens one of its own under lsm/, as above.
//
// What checks an index, compares it with its twin in memory or writes a copy
// of it laid out again is with the tests (grow_check_test.go).

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
	// growBucketDeleted holds the marks of the deleted vectors: strategy
	// Replace, doc id -> one byte. A deleted vector keeps its place in its
	// posting, in the map and among the routing codes: only the mark tells.
	growBucketDeleted = "postingedge_grow_deleted"
	// growBucketPending holds, where the index is asked to keep them
	// (growOpen.keepPending), the changes to the consolidated edges that wait
	// in memory: strategy Replace, doc id -> its list and the distances of
	// its edges, in the format growPendingFormat tells.
	growBucketPending = "postingedge_grow_pending"
	growPendingFormat = 1
	// growScratchSpare is the postings the scratch of a search has room for
	// beyond twice those there are, in an index whose doc ids have no bound.
	growScratchSpare = 1024
	// growSlotsFile, when an index has it, tells where the exact vector of
	// every doc id is in its vectors file. Only a copy of an index laid out
	// again by the experiments has one (reorganizeGrowIndex), and it does
	// not grow. An index that grows keeps the vector of doc id d at position
	// d, or has a table of slots of its own (growMeta.VectorGen).
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
	// RowsWithMembers, with Consolidate, has the row of edges of a posting
	// in the bucket of postings, as the one value of the key next to that
	// of its members, instead of in a bucket of its own (v5.3,
	// bench-report.md, "v5.3: la fila de aristas en un valor, junto a la de
	// miembros"). A Set cannot replace a value: writing the row again takes
	// the old one out, with a tombstone that carries it, and adds the new.
	RowsWithMembers bool `json:"rowsWithMembers,omitempty"`
	// VectorGen, when positive, tells that the exact vectors have slots: the
	// vector of a doc id is wherever the table of slots says, and not at the
	// position of its doc id. A vector that comes in takes the next slot, so
	// the file has no holes for the doc ids that were never given, and the
	// vectors can be written again in the order of their postings while the
	// index grows (grow_layout.go). The number is the generation of the two
	// files, vectors-<gen>.f32 and slots-<gen>.u32: laying the vectors out
	// again writes the next one, and the meta is what says which is current.
	VectorGen int `json:"vectorGen,omitempty"`
	// VectorsLaidOut is how many vectors the file held in the order of their
	// postings when it was last laid out. Those past them are in the order
	// they came.
	VectorsLaidOut int `json:"vectorsLaidOut,omitempty"`
	// RefreshedAt is how many vectors the index held when the last refresh of
	// its edges started, or when it was written: the next one is due when it
	// holds twice as many (grow_refresh.go).
	RefreshedAt int `json:"refreshedAt,omitempty"`
	// Merges counts the postings that joined another after a cleanup of the
	// deleted vectors (grow_clean.go). A merge changes no edge, so from the
	// first one on a vector may have an edge to another of its own posting,
	// which no insert and no split ever leaves: a search skips it.
	Merges int `json:"merges,omitempty"`
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
	RowWrites      growIO `json:"rowWrites"`
	RowWritesFull  int    `json:"rowWritesFull"`
	RowWritesSplit int    `json:"rowWritesSplit"`
	RowWritesClose int    `json:"rowWritesClose"`
	RowReads       growIO `json:"rowReads"`
	// RowTombstones counts the rows taken out of the bucket of postings to
	// be written again or because their posting was retired, where the rows
	// of edges live next to the members: each is a tombstone with the row.
	RowTombstones    growIO `json:"rowTombstones"`
	RowDists         growIO `json:"rowDistWrites"`
	RowDeletes       int    `json:"rowDeletes"`
	StoreRows        growIO `json:"storeRowReads"`
	PendingPeak      int64  `json:"pendingPeakMembers"`
	PendingPeakBytes int64  `json:"pendingPeakBytes"`
	PendingPeakPosts int    `json:"pendingPeakPostings"`
	// PendingLog counts the changes written to the bucket of what waits,
	// where the index keeps one.
	PendingLog growIO `json:"pendingLogWrites"`
	// Layouts counts the times the vectors were laid out again, and
	// LayoutVectors the vectors those wrote.
	Layouts       int   `json:"layouts,omitempty"`
	LayoutVectors int64 `json:"layoutVectors,omitempty"`

	AssignTime  time.Duration `json:"assignNs"`
	SearchTime  time.Duration `json:"searchNs"`
	WriteTime   time.Duration `json:"writeNs"`
	InverseTime time.Duration `json:"inverseNs"`
	SplitTime   time.Duration `json:"splitNs"`
	CycleTime   time.Duration `json:"cycleNs"`
	RowTime     time.Duration `json:"rowNs"`

	// By the clock: SearchPhase is the time the searches of the inserts
	// ran, several at once when the inserts go in batches (v5.2), and
	// ApplyPhase the time their writes ran, one after another. AssignTime
	// and SearchTime above add up what every search took by itself.
	SearchPhase time.Duration `json:"searchPhaseNs"`
	ApplyPhase  time.Duration `json:"applyPhaseNs"`
	// Of the inserts that went in batches: the batches; the plans whose
	// posting an insert of their batch had split, which chose again; and
	// the lists of edges a plan kept and needed that an insert of its batch
	// had changed, whose postings were read again, with the plans that met
	// any.
	Batches       int `json:"batches,omitempty"`
	BatchInserts  int `json:"batchInserts,omitempty"`
	PlansReassign int `json:"plansReassigned,omitempty"`
	PlansStale    int `json:"plansWithStaleLists,omitempty"`
	StaleLists    int `json:"staleLists,omitempty"`
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

	lsm *lsmkv.Store
	// shared tells that the store is not the index's own (growOpen.store):
	// whoever owns it flushes, compacts and closes it. names gives every
	// bucket of the index its name in such a store.
	shared bool
	names  func(bucket string) string
	// opened has the names of the buckets the index opened in its store.
	opened []string
	// journal, when the index keeps one, takes every write of the index
	// before its buckets and its files do (grow_journal.go). graphMarked
	// tells that the mark of a graph that changed is on disk, and recovered
	// what the index found of its journal when it was opened.
	journal     *growJournal
	graphMarked bool
	recovered   growRecovered
	// The buckets of the index, as its writes see them (growBucket): what a
	// step writes waits in the journal until the step ends.
	postings *growBucket
	posts    *growBucket
	routing  *growBucket
	dists    *growBucket
	// marks is the bucket of the marks of the deleted vectors, nil in an
	// index opened without deletes, and deleted those marks by doc id, the
	// ones a search reads (state.deleted).
	marks   *growBucket
	deleted []bool
	// edgeStore keeps the edges of the postings (growEdges): every list as a
	// value of the bucket of postings (growLists), unless the meta asks for
	// consolidated edges (growRows).
	edgeStore growEdges
	// edges is the bucket of the rows of edges, nil unless the edges are
	// consolidated in a bucket of their own, and pending what waits of every
	// posting. waiting is the bucket that holds what waits too, nil unless
	// the index was opened to keep it. Only a store of consolidated edges
	// touches them, and touched below.
	edges          *growBucket
	waiting        *growBucket
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
	// in the vectors file, diskNoSlot for a doc id without one. frozen tells
	// that it is the table of a copy laid out by the experiments, which is
	// searched and does not grow. Otherwise it is the table of an index whose
	// vectors have slots (growMeta.VectorGen): slots is its file, where a
	// vector that comes in writes its own, and nextSlot the slot the next
	// one takes.
	slotOf   []uint32
	frozen   bool
	slots    *os.File
	nextSlot uint32
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
	// rowCount, when set, has a fetch read the rows of its postings one
	// after another and count what each moved (growRowCount).
	rowCount *growRowCount
	// work, when set, takes the work of the search of every insert.
	work *SearchStats
	// lastPosting and lastEdges are the posting and the edges the last
	// insert gave its vector.
	lastPosting uint32
	lastEdges   []uint32

	// What the insert that is being applied keeps: the edges of the
	// postings its search loaded, and the exact vectors its rescore read.
	// They are those of its plan.
	kept     []growKept
	keptAt   map[uint32]int
	captured map[uint64][]float32
	// own is the plan of the insert of always, one at a time, used again
	// by every one of them.
	own *growPlan
	// touched has the postings whose edges the insert that runs changed.
	touched []uint32
	// batch has the plans of a batch of inserts (v5.2), used again by
	// every batch, and batchDirty the docs whose list of edges an insert
	// of the batch that is being applied has changed: nil unless a batch
	// is being applied. batchStale tells that the insert that is being
	// applied met one of them among the lists its plan kept.
	batch      []*growPlan
	batchDirty map[uint64]struct{}
	batchStale bool
	// batchVerify, for the tests, has every list of edges that an insert of
	// a batch takes from what its plan kept read again from the index, and
	// fails the insert if the two differ: a plan kept a list that an insert
	// of its batch changed, and nobody noticed. batchVerified counts them.
	batchVerify   bool
	batchVerified int
	// batchApplied, with batchVerify, has the doc ids of the last batch in
	// the order their writes were applied.
	batchApplied []uint64
	// vectorReads counts the exact vectors read from the file.
	vectorReads atomic.Int64
	sinceCycle  int
	// deletedCount is how many vectors are marked deleted, and
	// scratchPostings the postings the scratch of a search has room for.
	deletedCount    int
	scratchPostings int
	// cleaned counts what the passes over the deleted vectors did
	// (grow_clean.go).
	cleaned growCleanStats
	// seq counts the writes the index has applied: an insert, a refresh of
	// the edges of a vector, a step of a cleanup. listSeq is, by posting, the
	// write that last changed a list of edges of it, and removedSeq the last
	// one that took a vector out. A plan knows the count it searched with
	// (growPlan.seq): the search of an insert can run while other inserts
	// write, and what they wrote since is not what it kept. applying is the
	// count of the plan that is being applied, and reread the postings its
	// writes have read again.
	seq        uint64
	listSeq    []uint64
	removedSeq uint64
	applying   uint64
	reread     []uint32
	// sinceMerge counts the writes of the index since the store was last
	// asked to leave the bucket of postings as one segment, merging tells
	// that it is being asked now, and merges how many times it got there
	// (persistent_merge.go).
	sinceMerge int
	merging    bool
	merges     int
	// verifyKept, for the tests, has every list of edges that a write takes
	// from what its plan kept read again from the index, and fails the write
	// if the two differ, as batchVerify does for a batch: a plan kept a list
	// that a write changed since, and nobody noticed. batchVerified counts
	// them.
	verifyKept bool
	// builtAt is, by doc id, how many vectors the index held when the edges
	// of each were last built, built the file that holds it, and refreshed
	// what the refreshes of edges did (grow_refresh.go).
	builtAt   []uint32
	built     *os.File
	refreshed growRefreshStats
	// insertEF, when positive, is the ef of the search of an insert for its
	// edges. Without it that search runs with the ef of the index, as the
	// queries do.
	insertEF int
}

// growPlan is what the search of an insert leaves for its writes: where the
// vector goes, its neighbors, and what the search read on the way, which the
// writes use again. A search writes nothing but its plan, so the searches of
// several inserts can run at once (v5.2).
type growPlan struct {
	docID  uint64
	vector []float32
	// entry are the postings the centroid graph gave the vector, the first
	// of them its posting.
	entry []uint64
	// neighbors and dists are what the search returned.
	neighbors []uint64
	dists     []float32
	// kept has the edges of the postings the search loaded, and keptAt
	// where those of each posting are.
	kept   []growKept
	keptAt map[uint32]int
	// captured has the exact vectors the rescore of the search read, which
	// reads them from several goroutines, and vectorReads counts them.
	captureMu   sync.Mutex
	captured    map[uint64][]float32
	vectorReads int
	searched    diskSearchStats
	// work, when set, takes the work of the search.
	work *SearchStats
	// assignTime and searchTime are what the centroid graph and the search
	// of the neighbors took.
	assignTime, searchTime time.Duration
	// selfMissing, in the plan of a refresh, tells that the search of the
	// vector did not return the vector itself.
	selfMissing bool
	// seq is the count of the writes the index had applied when the plan
	// searched (growIndex.seq).
	seq uint64
}

func newGrowPlan() *growPlan {
	return &growPlan{keptAt: make(map[uint32]int), captured: make(map[uint64][]float32)}
}

// growPlanKey is how the search of a plan tells the read of an exact vector
// whose it is.
type growPlanKey struct{}

// keep copies what the inverse edges of the insert need of a posting its
// search loaded.
func (p *growPlan) keep(q uint32, dp *diskPosting) {
	at := len(p.kept)
	if at < cap(p.kept) {
		p.kept = p.kept[:at+1]
	} else {
		p.kept = append(p.kept, growKept{})
	}
	kept := &p.kept[at]
	kept.docIDs = append(kept.docIDs[:0], dp.docIDs...)
	kept.edgeStart = append(kept.edgeStart[:0], dp.edgeStart...)
	kept.edges = append(kept.edges[:0], dp.edges...)
	p.keptAt[q] = at
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

	// store, when set, is the store the index keeps its buckets in, in place
	// of one of its own under its directory: the store of a shard. Its owner
	// flushes it, compacts it and closes it, with the cycles of the node,
	// and the three options above are not the index's to give. makeOptions
	// gives the options the owner wants on every bucket, and names the name
	// of every bucket of the index in that store, where other indexes keep
	// theirs.
	store       *lsmkv.Store
	makeOptions lsmkv.MakeBucketOptions
	names       func(bucket string) string
	// deletes opens the bucket of the marks of the deleted vectors. An index
	// opened without it has none and deletes nothing.
	deletes bool
	// keepPending has an index with consolidated edges write every change
	// that waits in memory to a bucket too, as it is made, and read them
	// back when it opens: what a sync leaves on disk is then the whole
	// index. Without it what waits is written when the index closes, and an
	// index that did not close has lost it: the experiments, which close
	// every index they grow.
	keepPending bool
	// cycles are the cycles of the node that keep the centroid graph: none
	// in the experiments.
	cycles centroidCycles
	// journal has every write of the index go through its journal
	// (grow_journal.go), so that what a process that dies leaves on disk is
	// the index as it was after its last whole write. An index opened
	// without it still reads the journal it finds, and then has none.
	journal bool
}

// growBlock is a row of edges decoded: the members it has a list of, in its
// order, and where the list of each starts.
type growBlock struct {
	ids   []uint32
	start []uint32
	edges []uint32
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
	// rowsWithMembers has those rows in the bucket of postings, next to the
	// members (growMeta.RowsWithMembers).
	rowsWithMembers bool
	// slots gives the exact vectors slots (growMeta.VectorGen): the base is
	// written with the vectors of every posting in a row.
	slots bool
	// open says where the buckets of the index go: the zero value is a store
	// of its own under dir.
	open growOpen
}

// writeGrowIndex writes an index built in batch to dir, which must exist and
// be empty, in the format that grows: the base the inserts start from. st is
// the state of the index, keyed by slot. vectorOf gives the vector of a doc
// id as the index stores it.
func writeGrowIndex(ctx context.Context, logger logrus.FieldLogger, dir string, st *state, cfg growConfig,
	provider distancer.Provider, vectorOf func(docID uint64) []float32,
) (err error) {
	if cfg.rowsWithMembers && cfg.consolidate <= 0 {
		// Worded without the verb the experiments had here: the linter of
		// goroutines reads it as a statement that starts one.
		return errors.New("the rows of edges are next to the members only with consolidated edges")
	}
	if st == nil {
		return ErrNotBuilt
	}
	store := st.postings
	if store.lists != nil {
		return errors.New("the index is keyed by doc id already: the base is an index built in batch")
	}
	quantizer, routingBits, sketchScan, err := diskCodecs(st)
	if err != nil {
		return err
	}
	if sketchScan {
		return errors.New("the postings carry the code of the endpoints: the index that grows writes the full code of its members")
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
		Consolidate: cfg.consolidate, RowsWithMembers: cfg.rowsWithMembers,
	}
	var slotOf []uint32
	if cfg.slots {
		// The base is laid out as it is written: its vectors go in the order
		// of its postings.
		g.meta.VectorGen, g.meta.VectorsLaidOut = 1, n
		slotOf = make([]uint32, cfg.keys)
		for i := range slotOf {
			slotOf[i] = diskNoSlot
		}
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

	if err := g.openStore(ctx, cfg.open); err != nil {
		return err
	}
	defer func() {
		// Of a store it was given, only the buckets it opened.
		if shut := g.closeStore(ctx); err == nil {
			err = errors.Wrap(shut, "close the store")
		}
	}()
	if g.vectors, err = os.OpenFile(path(growVectorsName(g.meta.VectorGen)), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return err
	}
	defer func() {
		if closed := g.vectors.Close(); err == nil {
			err = closed
		}
	}()

	raw := make([]byte, 4*st.dims)
	written := uint32(0)
	var dists []float32
	var edges []uint32
	for p := range postings {
		from, to := store.start[p], store.start[p+1]
		members := make([][]byte, 0, to-from)
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
			at := docID
			if slotOf != nil {
				at, slotOf[docID] = uint64(written), written
				written++
			}
			if _, err := g.vectors.WriteAt(raw, int64(at)*int64(len(raw))); err != nil {
				return errors.Wrapf(err, "write the vector of doc %d", docID)
			}
		}
		if err := g.postings.SetAdd(diskKey(uint32(p), diskBlockMembers), members); err != nil {
			return errors.Wrapf(err, "write the members of posting %d", p)
		}
		if err := g.edgeStore.writePosting(uint32(p), store.docIDs[from:to], rowLists, false); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	// The edges of every vector of the base are those of the base.
	builtAt := make([]uint32, cfg.keys)
	for _, docID := range store.docIDs {
		builtAt[docID] = uint32(n)
	}
	if err := writeBuilt(path(growBuiltFile), builtAt); err != nil {
		return errors.Wrap(err, "write when the edges were built")
	}
	g.meta.RefreshedAt = n
	if slotOf != nil {
		if err := writeSlots(path(growSlotsName(g.meta.VectorGen)), slotOf); err != nil {
			return errors.Wrap(err, "write where the vectors are")
		}
	} else if err := g.vectors.Truncate(int64(cfg.keys) * int64(len(raw))); err != nil {
		// The file has room for every key: a vector is read at its doc id.
		return err
	}
	if err := g.flushAll(); err != nil {
		return err
	}
	return g.writeMeta()
}

// writeMeta writes the meta whole to a file next to it and then gives it its
// name, so that a meta that is half written is never the meta of the index.
func (g *growIndex) writeMeta() error {
	raw, err := json.MarshalIndent(g.meta, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(g.dir, diskMetaFile)
	if err := os.WriteFile(path+".tmp", raw, 0o644); err != nil {
		return errors.Wrap(err, "write the meta")
	}
	return errors.Wrap(os.Rename(path+".tmp", path), "write the meta")
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

// openStore opens the buckets of the index: the four it has always, the one
// of the marks of the deleted vectors if asked for, and those of the store of
// its edges.
//
// In a store of its own, under its directory, the flush and compaction cycles
// are the index's to call (cycle): nothing runs them in the background. A
// memtable is flushed past the default threshold of the store, and never
// because of the time it has been dirty. In a store it is given
// (growOpen.store) the cycles are those of its owner.
func (g *growIndex) openStore(ctx context.Context, opts growOpen) error {
	var store *lsmkv.Store
	// common goes on every bucket, after its strategy and its own options.
	var common []lsmkv.BucketOption
	makeOptions := lsmkv.MakeBucketOptions(lsmkv.MakeRegularBucketOptions)
	if opts.store != nil {
		store, g.shared, g.names = opts.store, true, opts.names
		if opts.makeOptions != nil {
			makeOptions = opts.makeOptions
		}
	} else {
		root := filepath.Join(g.dir, diskStoreDir)
		g.flush = cyclemanager.NewCallbackGroup("flush", g.logger, 1)
		g.compaction = cyclemanager.NewCallbackGroup("compaction", g.logger, 1)
		own, err := lsmkv.New(root, root, g.logger, opts.metrics, nil, g.compaction, cyclemanager.NewCallbackGroupNoop(), g.flush)
		if err != nil {
			return errors.Wrap(err, "open the store")
		}
		store = own
		common = []lsmkv.BucketOption{
			lsmkv.WithPread(opts.pread), lsmkv.WithForceCompaction(!opts.leveled), lsmkv.WithDirtyThreshold(1000 * time.Hour),
		}
	}
	g.lsm = store
	// If a bucket fails to open, a store of its own is closed, and of one
	// that was given the buckets opened so far.
	failed := func(err error) error {
		_ = g.closeStore(ctx)
		return err
	}
	open := func(bucket string, options []lsmkv.BucketOption) error {
		name := g.bucketName(bucket)
		if g.shared && store.Bucket(name) != nil {
			// The store would hand it back as whoever opened it left it.
			return errors.Errorf("the bucket %s is open in the store already", name)
		}
		if err := store.CreateOrLoadBucket(ctx, name, options...); err != nil {
			return errors.Wrapf(err, "open the bucket %s", name)
		}
		g.opened = append(g.opened, name)
		return nil
	}

	postingOptions := append([]lsmkv.BucketOption{lsmkv.WithShouldSkipKeyFunction(g.skipKey)}, common...)
	if g.shared {
		// As the postings bucket of HFresh: the segments left over compact
		// too.
		postingOptions = append(postingOptions, lsmkv.WithForceCompaction(true))
	}
	if err := open(growBucketPostings, makeOptions(lsmkv.StrategySetCollection, postingOptions...)); err != nil {
		return failed(err)
	}
	replace := []string{growBucketMap, growBucketRouting, growBucketDists}
	if opts.deletes {
		replace = append(replace, growBucketDeleted)
	}
	for _, name := range replace {
		if err := open(name, makeOptions(lsmkv.StrategyReplace, common...)); err != nil {
			return failed(err)
		}
	}
	bucket := func(target growTarget, name string) *growBucket {
		return g.bucketOf(target, store.Bucket(g.bucketName(name)))
	}
	g.postings, g.posts = bucket(growTargetPostings, growBucketPostings), bucket(growTargetMap, growBucketMap)
	g.routing, g.dists = bucket(growTargetRouting, growBucketRouting), bucket(growTargetDists, growBucketDists)
	if opts.deletes {
		g.marks = bucket(growTargetDeleted, growBucketDeleted)
	}

	g.edgeStore = growLists{g}
	if g.meta.Consolidate > 0 {
		if !g.meta.RowsWithMembers {
			if err := open(growBucketEdges, makeOptions(lsmkv.StrategyReplace, common...)); err != nil {
				return failed(err)
			}
			g.edges = bucket(growTargetEdges, growBucketEdges)
		}
		if opts.keepPending {
			if err := open(growBucketPending, makeOptions(lsmkv.StrategyReplace, common...)); err != nil {
				return failed(err)
			}
			g.waiting = bucket(growTargetPending, growBucketPending)
		}
		g.pending = make(map[uint32]*growPending)
		g.edgeStore = growRows{g}
	}
	return nil
}

// shutdownBucket closes a bucket of a store the index does not own, and takes
// it out of it. One that is not there, or a store that was closed with it
// inside, has nothing left to close.
func shutdownBucket(ctx context.Context, store *lsmkv.Store, name string) error {
	if store.Bucket(name) == nil {
		return nil
	}
	err := store.ShutdownBucket(ctx, name)
	if errors.Is(err, lsmkv.ErrBucketNotFound) {
		return nil
	}
	return err
}

// bucketName is the name of a bucket of the index in its store: its own in a
// store of its own, and the one it is given in a store it shares.
func (g *growIndex) bucketName(bucket string) string {
	if g.names != nil {
		return g.names(bucket)
	}
	return bucket
}

func (g *growIndex) buckets() []*lsmkv.Bucket {
	buckets := []*lsmkv.Bucket{g.postings.Bucket, g.posts.Bucket, g.routing.Bucket, g.dists.Bucket}
	if g.marks != nil {
		buckets = append(buckets, g.marks.Bucket)
	}
	return append(buckets, g.edgeStore.buckets()...)
}

// consolidated tells whether the edges of a posting are one row, written
// whole (v5.1 and v5.3).
func (g *growIndex) consolidated() bool { return g.meta.Consolidate > 0 }

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
// background routines would. In a store the index was given they are not
// its to call.
func (g *growIndex) cycle() {
	if g.shared {
		return
	}
	never := func() bool { return false }
	g.flush.CycleCallback(never)
	g.compaction.CycleCallback(never)
}

// compactAll flushes everything and compacts until the store finds nothing
// more to compact, which leaves every bucket in one segment unless the store
// was opened with its leveled compaction. It returns the rounds that took.
func (g *growIndex) compactAll() (int, error) {
	if g.shared {
		return 0, errors.New("the store is not the index's own: its owner compacts it")
	}
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
	// What the journal holds happened after that meta was written: the index
	// is the one of its last record.
	journaled, err := readGrowJournal(path(growJournalFile))
	if err != nil {
		return nil, err
	}
	if len(journaled) > 0 {
		last := journaled[len(journaled)-1].meta
		g.meta = growMeta{}
		if err := json.Unmarshal(last, &g.meta); err != nil {
			return nil, errors.Wrap(err, "parse the meta of the last record of the journal")
		}
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

	// The writes of the journal to the files of the index, before any of
	// them is read.
	if err := replayGrowFiles(dir, meta, journaled); err != nil {
		return nil, err
	}
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
	graph, err := openCentroidGraphWith(ctx, logger, path(diskGraphDir), centroids, opts.cycles)
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
	if g.vectors, err = os.OpenFile(path(growVectorsName(meta.VectorGen)), os.O_RDWR, 0o644); err != nil {
		return nil, errors.Wrap(err, "open the vectors")
	}
	if meta.VectorGen > 0 {
		if err := g.openSlots(); err != nil {
			return nil, err
		}
	} else if _, err := os.Stat(path(growSlotsFile)); err == nil {
		if g.slotOf, err = readArray(path(growSlotsFile), 4, meta.Keys, binary.LittleEndian.Uint32); err != nil {
			return nil, errors.Wrap(err, "read where the vectors are")
		}
		g.frozen = true
	}
	if err := g.openStore(ctx, opts); err != nil {
		return nil, err
	}
	// And its writes to the buckets, before any of them is read.
	for r := range journaled {
		for e := range journaled[r].entries {
			if entry := &journaled[r].entries[e]; entry.kind != growEntryFile {
				if err := g.hand(entry); err != nil {
					return nil, errors.Wrapf(err, "write again record %d of the journal", r)
				}
			}
		}
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
	err = growScan(g.posts.Bucket, func(key, value []byte) error {
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
	err = growScan(g.routing.Bucket, func(key, value []byte) error {
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
	if meta.VectorGen > 0 {
		for docID, held := range g.live {
			if held && g.slotOf[docID] == diskNoSlot {
				return nil, errors.Errorf("doc %d is in the map and has no vector in the file", docID)
			}
		}
	}
	g.retired = make([]bool, meta.Postings)
	for p, size := range g.size {
		g.retired[p] = size == 0
	}
	g.listSeq = make([]uint64, meta.Postings)
	// How much was written since the bucket of postings was last one
	// segment is not known to an index that opens: it counts as all of it.
	g.sinceMerge = live
	if err := g.openBuilt(); err != nil {
		return nil, err
	}
	if g.marks != nil {
		// The marks of the deleted vectors, which are still in their postings
		// and in the map. A search scores such a member and follows its
		// edges, so that it goes on routing, and never returns it.
		g.deleted = make([]bool, meta.Keys)
		err = growScan(g.marks.Bucket, func(key, _ []byte) error {
			if len(key) != 4 {
				return errors.Errorf("a mark of a deleted vector with a key of %d bytes", len(key))
			}
			docID := binary.BigEndian.Uint32(key)
			if int(docID) >= meta.Keys || !g.live[docID] {
				return errors.Errorf("a mark of a deleted vector for doc %d, which is not in the map", docID)
			}
			g.deleted[docID] = true
			g.deletedCount++
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "read the marks of the deleted vectors")
		}
		g.st.deleted, g.st.deletedAsSources = g.deleted, true
	}

	// Every split adds two postings, and there is at most one per insert.
	g.scratchPostings = meta.Postings + 2*(meta.Keys-live) + 2
	if g.shared {
		// The doc ids a shard gives out have no bound, so neither do the
		// postings there can be: the scratch of a search has room for twice
		// those there are, and grows when a split passes it (restate).
		g.scratchPostings = 2*meta.Postings + growScratchSpare
	}
	capacity := g.scratchPostings
	g.st.postings = &postingStore{codeWords: meta.ScanCodeWords, lists: []postingList{}}
	g.st.side = side
	g.st.visited = visited.NewPool(meta.Keys)
	g.st.scratch.New = func() any { return newSearchScratch(capacity) }
	g.rounds.New = func() any { return &diskRound{} }
	g.sums = make([]float64, meta.Dims)
	g.own = newGrowPlan()
	g.kept, g.keptAt, g.captured = g.own.kept, g.own.keptAt, g.own.captured
	if g.consolidated() {
		// What waited when the index was last synced, where it keeps it.
		if err := g.loadPending(); err != nil {
			return nil, err
		}
	}

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
	if err := g.openJournal(ctx, opts, len(journaled)); err != nil {
		return nil, err
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
	if err := g.writeMeta(); err != nil {
		return err
	}
	// Everything the journal tells is now in the system by its own way.
	if err := g.emptyJournal(); err != nil {
		return err
	}
	return g.graphSynced()
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
		if g.waiting == nil {
			// What waits in memory of the edges is nowhere else. Where it
			// is kept in a bucket it goes on waiting there, as it does
			// through every sync, and no row is written for closing.
			keep(g.flushPending())
		}
		keep(g.sync())
	}
	keep(g.release(ctx))
	return first
}

// release lets go of what the index holds and writes nothing of its own: the
// meta stays as the last sync left it. It is how close ends, and what is left
// to do with an index that a write left halfway.
func (g *growIndex) release(ctx context.Context) error {
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	if g.ix != nil {
		keep(g.ix.Shutdown(ctx))
	}
	if g.st != nil && g.st.centroids != nil {
		keep(g.st.centroids.shutdown(ctx))
	}
	if g.lsm != nil {
		keep(g.closeStore(ctx))
	}
	for _, file := range []*os.File{g.vectors, g.slots, g.built, g.centroids} {
		if file != nil {
			keep(file.Close())
		}
	}
	if g.journal != nil && g.journal.file != nil {
		keep(g.journal.file.Close())
	}
	return first
}

// closeStore closes the store of the index. Of a store it was given it closes
// only its own buckets, which leave the store: they were opened with what
// tells this index which rows nothing reads any more (skipKey), and whoever
// opens the index next opens them again with its own.
func (g *growIndex) closeStore(ctx context.Context) error {
	if !g.shared {
		return g.lsm.Shutdown(ctx)
	}
	var first error
	for _, name := range g.opened {
		if err := shutdownBucket(ctx, g.lsm, name); err != nil && first == nil {
			first = errors.Wrapf(err, "close the bucket %s", name)
		}
	}
	g.opened = nil
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
	if err := g.readFile(growTargetVectors, raw, int64(slot)*int64(4*dims)); err != nil {
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
func (g *growIndex) readVector(ctx context.Context, docID uint64, container *common.VectorSlice, _ common.BucketView,
) ([]float32, error) {
	if docID >= uint64(len(g.live)) || !g.live[docID] {
		return nil, storobj.NewErrNotFoundf(docID, "the index does not hold it")
	}
	var err error
	container.Buff, container.Slice, err = g.exact(docID, container.Buff, container.Slice)
	if err != nil {
		return nil, err
	}
	if plan, _ := ctx.Value(growPlanKey{}).(*growPlan); plan != nil {
		plan.captureMu.Lock()
		plan.captured[docID] = slices.Clone(container.Slice)
		plan.vectorReads++
		plan.captureMu.Unlock()
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
		// A member that a cleanup took out (grow_clean.go) is under a
		// tombstone until the store compacts it away, and the row as the
		// store holds it has more values than the posting has members: it
		// is read again with its tombstones resolved, which keeps the order
		// of those that are left. Without a cleanup the two counts are the
		// same, and this is not on the way of a search.
		if values, err = g.postings.SetList(diskKey(p, diskBlockMembers)); err != nil {
			return errors.Wrapf(err, "read row %d of posting %d", diskBlockMembers, p)
		}
		n = len(values)
	}
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

// fetchLists reads what the store holds of the edges of a posting, which
// joinLists then lays out by member.
func (g *growIndex) fetchLists(p uint32, dp *diskPosting) error {
	return g.edgeStore.fetch(p, dp)
}

// growCounters is how many counters a growRowCount follows.
const growCounters = 2

// growRowCount has the fetches of a growing index read the rows of their
// postings one after another, not all at once, and adds up by row what each
// read moved some counters of the caller's: what the store counts as read,
// or what the system read from disk for the process. Reading one row at a
// time is what tells the two rows of a posting apart: the counters are of
// the whole process. It is for one search at a time, and times nothing.
type growRowCount struct {
	// edgesFirst reads the row of edges of a posting before its members.
	// Where the two rows lie together on disk, the first one read brings
	// part of the other.
	edgesFirst bool
	// read returns the counters, which only grow.
	read func() ([growCounters]uint64, error)
	// rows adds up what the counters moved by, by row (diskBlockMembers and
	// diskBlockEdges), and loads counts the postings fetched.
	rows  [3][growCounters]uint64
	loads int
}

// fetchCounting is fetch with the rows read one after another, each counted.
func (g *growIndex) fetchCounting(round *diskRound, picks []scored) error {
	c := g.rowCount
	order := [2]byte{diskBlockMembers, diskBlockEdges}
	if c.edgesFirst {
		order = [2]byte{diskBlockEdges, diskBlockMembers}
	}
	for i, pick := range picks {
		p, dp := uint32(pick.id), &round.postings[i]
		dp.bytes, dp.took = [3]int{}, [3]time.Duration{}
		for _, block := range order {
			before, err := c.read()
			if err != nil {
				return err
			}
			if block == diskBlockMembers {
				err = g.fetchMembers(p, dp)
			} else {
				err = g.fetchLists(p, dp)
			}
			if err != nil {
				return err
			}
			after, err := c.read()
			if err != nil {
				return err
			}
			for at := range after {
				c.rows[block][at] += after[at] - before[at]
			}
		}
		c.loads++
	}
	return nil
}

// fetch reads the two rows of every posting given, all at once, and leaves
// each posting ready to be visited.
func (g *growIndex) fetch(round *diskRound, picks []scored) error {
	if cap(round.postings) < len(picks) {
		round.postings = append(round.postings[:cap(round.postings)], make([]diskPosting, len(picks)-cap(round.postings))...)
	}
	round.postings = round.postings[:len(picks)]
	if g.rowCount != nil {
		if err := g.fetchCounting(round, picks); err != nil {
			return err
		}
	} else {
		eg := enterrors.NewErrorGroupWrapper(g.logger)
		if round.inFlight > 0 && round.inFlight < 2*len(picks) {
			// The budget of the query does not reach to all the rows of the
			// round at once: they are read as it allows.
			eg.SetLimit(round.inFlight)
		}
		for i, pick := range picks {
			p, dp := uint32(pick.id), &round.postings[i]
			dp.bytes, dp.took = [3]int{}, [3]time.Duration{}
			eg.Go(func() error {
				return timedRead(&dp.took[diskBlockMembers], func() error { return g.fetchMembers(p, dp) })
			})
			eg.Go(func() error {
				return timedRead(&dp.took[diskBlockEdges], func() error { return g.fetchLists(p, dp) })
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	for i, pick := range picks {
		dp := &round.postings[i]
		// Laying the edges out by member is part of reading their row.
		clock := time.Now()
		if err := g.joinLists(uint32(pick.id), dp); err != nil {
			return err
		}
		dp.took[diskBlockEdges] += time.Since(clock)
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

// keep copies what the inverse edges of the insert that is being applied
// need of a posting it reads again.
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
	switch {
	case !found:
	case g.batchDirty != nil:
		if _, changed := g.batchDirty[u]; changed {
			// An insert of the batch changed this list after the plan kept
			// it: its posting is read again, as it is now.
			found = false
			g.batchStale = true
			g.stats.StaleLists++
		}
	case g.listSeq[q] > g.applying && g.listSeq[q] != g.seq && !slices.Contains(g.reread, q):
		// A write that came between the search of the plan and its own
		// writes changed a list of this posting: what the plan kept of it
		// is read again, once. What this write itself changed of it is in
		// what it keeps.
		found = false
		g.stats.StaleLists++
	}
	if !found {
		g.reread = append(g.reread, q)
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
	list := kept.edges[kept.edgeStart[m]:kept.edgeStart[m+1]]
	if found && (g.batchVerify && g.batchDirty != nil || g.verifyKept) {
		var dp diskPosting
		if err := g.fetchMembers(q, &dp); err != nil {
			return nil, err
		}
		if err := g.fetchLists(q, &dp); err != nil {
			return nil, err
		}
		if err := g.joinLists(q, &dp); err != nil {
			return nil, err
		}
		now := slices.Index(dp.docIDs, u)
		if now < 0 || !slices.Equal(dp.edges[dp.edgeStart[now]:dp.edgeStart[now+1]], list) {
			return nil, errors.Errorf("the plan of the insert kept the edges %v of doc %d in posting %d, and the index has others", list, u, q)
		}
		g.batchVerified++
	}
	return list, nil
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

// extend gives the index room for the doc ids [0, keys): the tables it keeps
// in memory by doc id grow, and the meta with them. The vectors file grows
// by itself, as vectors are written past its end. It must not run next to a
// search or an insert: a search holds the tables it started with.
func (g *growIndex) extend(keys int) error {
	if keys <= g.meta.Keys {
		return nil
	}
	if g.frozen {
		return errors.New("the index was laid out again to be searched: it does not grow")
	}
	if keys > maxVectors {
		return errors.Errorf("room for %d doc ids, the index addresses up to %d", keys, maxVectors)
	}
	if g.slotOf != nil {
		slotOf := make([]uint32, keys)
		copy(slotOf, g.slotOf)
		for i := g.meta.Keys; i < keys; i++ {
			slotOf[i] = diskNoSlot
		}
		g.slotOf = slotOf
	}
	if g.builtAt != nil {
		builtAt := make([]uint32, keys)
		copy(builtAt, g.builtAt)
		g.builtAt = builtAt
	}
	old := g.st.side
	side := &sidecar{
		codeWords: old.codeWords, codes: make([]uint64, keys*old.codeWords), postingOf: make([]uint32, keys),
	}
	copy(side.codes, old.codes)
	copy(side.postingOf, old.postingOf)
	for i := g.meta.Keys; i < keys; i++ {
		side.postingOf[i] = unplaced
	}
	live := make([]bool, keys)
	copy(live, g.live)
	g.live = live
	if g.deleted != nil {
		deleted := make([]bool, keys)
		copy(deleted, g.deleted)
		g.deleted = deleted
	}
	g.meta.Keys = keys
	g.restate(side)
	return nil
}

// restate gives the index a new state over the given sidecar, with the marks
// of the deleted vectors as they are now and a scratch with room for the
// postings there can be. What a search keeps of a state is sized when the
// state is made, so whatever outgrows it makes a new one. It must not run
// next to a search.
func (g *growIndex) restate(side *sidecar) {
	old := g.st
	if !g.shared {
		g.scratchPostings = g.meta.Postings + 2*(g.meta.Keys-g.meta.Live) + 2
	} else if g.meta.Postings+2 > g.scratchPostings {
		g.scratchPostings = 2*g.meta.Postings + growScratchSpare
	}
	st := &state{
		dims:                 old.dims,
		quantizer:            old.quantizer,
		scanQuantizer:        old.scanQuantizer,
		centroids:            old.centroids,
		postings:             old.postings,
		side:                 side,
		visited:              old.visited,
		build:                old.build,
		deleted:              g.deleted,
		skipDeletedEndpoints: old.skipDeletedEndpoints,
		deletedAsSources:     old.deletedAsSources,
	}
	capacity := g.scratchPostings
	st.scratch.New = func() any { return newSearchScratch(capacity) }
	g.st = st
}

// remove marks a vector deleted, on disk and in memory. It keeps its place
// in its posting, its routing code and its edges, and the edges that lead
// to it stay: a search scores it and follows its edges, so that it goes on
// routing, and never returns it (the cell 1b of v6.0, bench-report.md,
// "v6.0: deletes en memoria"). Nothing is ever taken out here. It tells
// whether the vector was there to be marked: one the index never held, or
// one that is marked already, is not an error. It must not run next to a
// search or an insert.
func (g *growIndex) remove(docID uint64) (bool, error) {
	if g.marks == nil {
		return false, errors.New("the index was opened without deletes")
	}
	if docID >= uint64(len(g.live)) || !g.live[docID] || g.deleted[docID] {
		return false, nil
	}
	// One write: a step of its own, unless whoever called marks several in
	// one.
	if err := g.marks.Put(growDocKey(docID), []byte{1}); err != nil {
		return false, errors.Wrapf(err, "write the mark of doc %d", docID)
	}
	g.deleted[docID] = true
	g.deletedCount++
	return true, nil
}

// holds tells whether the index holds a vector under a doc id that is not
// marked deleted.
func (g *growIndex) holds(docID uint64) bool {
	return docID < uint64(len(g.live)) && g.live[docID] && (g.deleted == nil || !g.deleted[docID])
}

// insertable tells whether the index can take a vector under a doc id.
func (g *growIndex) insertable(docID uint64, vector []float32) error {
	if g.frozen {
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
	return nil
}

// insert adds one vector by the incremental path of the streaming index
// (streamIndex.insert), with inverse edges and a split that touches no edge,
// reading and writing the store. The vector must be normalized as the index
// stores it. The search parameters of the edge search are the index's own.
// It is the search of the vector (plan) and then its writes (apply).
func (g *growIndex) insert(ctx context.Context, docID uint64, vector []float32) error {
	if err := g.insertable(docID, vector); err != nil {
		return err
	}
	clock := time.Now()
	reads, bytes, err := g.counted()
	if err != nil {
		return err
	}
	plan := g.own
	plan.work = g.work
	if err := g.plan(ctx, plan, docID, vector); err != nil {
		return err
	}
	readsAfter, bytesAfter, err := g.counted()
	if err != nil {
		return err
	}
	g.stats.StoreSearch.add(int(readsAfter-reads), int(bytesAfter-bytes))
	g.stats.SearchPhase += time.Since(clock)
	clock = time.Now()
	err = g.apply(ctx, plan)
	g.stats.ApplyPhase += time.Since(clock)
	return err
}

// plan is the part of an insert that writes nothing: the posting of the
// vector, from the centroid graph, and the search for its edges, which
// leaves in the plan the edges of what it loaded and the vectors it
// rescored. It reads the index and touches nothing of it, so several can run
// at once as long as no insert is being applied.
func (g *growIndex) plan(ctx context.Context, plan *growPlan, docID uint64, vector []float32) error {
	st := g.st
	plan.docID, plan.vector, plan.seq = docID, vector, g.seq
	plan.kept = plan.kept[:0]
	clear(plan.keptAt)
	clear(plan.captured)
	plan.vectorReads = 0

	clock := time.Now()
	entry, err := st.centroids.nearest(ctx, vector, max(int(g.ix.entryPostings.Load()), 1))
	if err != nil {
		return err
	}
	if len(entry) == 0 {
		return errors.New("the centroid graph found no centroid")
	}
	for _, e := range entry {
		if e >= uint64(len(g.retired)) || g.retired[e] {
			return errors.Errorf("the centroid graph returned posting %d, which is not live", e)
		}
	}
	plan.entry = entry
	plan.assignTime = time.Since(clock)

	clock = time.Now()
	search := g.searcher()
	search.loaded = plan.keep
	search.ef = g.insertEF
	plan.searched = diskSearchStats{}
	if plan.work != nil {
		*plan.work = SearchStats{}
		plan.searched.Work = plan.work
	}
	plan.neighbors, plan.dists, err = search.run(context.WithValue(ctx, growPlanKey{}, plan), vector, g.meta.Neighbors,
		entry, &plan.searched)
	if err != nil {
		return errors.Wrapf(err, "search the neighbors of doc %d", docID)
	}
	plan.searchTime = time.Since(clock)
	return nil
}

// apply is the part of an insert that writes: the vector in its posting with
// its edges, the inverse edges, the split and, with consolidated edges, the
// rows that are due. One at a time, in the order of the inserts.
func (g *growIndex) apply(ctx context.Context, plan *growPlan) error {
	return g.step(func() error { return g.applyWrites(ctx, plan) })
}

// applyWrites is apply, inside its step.
func (g *growIndex) applyWrites(ctx context.Context, plan *growPlan) error {
	st := g.st
	docID, vector := plan.docID, plan.vector
	// A neighbor that was marked deleted since the search is not one.
	neighbors, dists := g.heldNeighbors(plan.neighbors, plan.dists)
	p := uint32(plan.entry[0])
	// What the search of the plan left is what the writes read from, but for
	// what was written since (listOf).
	g.begin(plan)
	g.kept, g.keptAt, g.captured = plan.kept, plan.keptAt, plan.captured
	defer func() { plan.kept = g.kept }()
	searched := &plan.searched
	g.stats.AssignTime += plan.assignTime
	g.stats.SearchLoads += searched.Postings
	g.stats.SearchRounds += searched.Rounds
	g.stats.SearchRows.add(searched.Blocks, searched.BlockBytes)
	g.stats.SearchVectors.add(plan.vectorReads, plan.vectorReads*4*g.meta.Dims)
	g.stats.SearchTime += plan.searchTime
	reads, bytes, err := g.counted()
	if err != nil {
		return err
	}
	var readsAfter, bytesAfter uint64

	// The vector, in its posting, with its edges: of the neighbors, those
	// outside its posting. The distance of every edge is the exact one from
	// the vector to its endpoint, which is what a later inverse edge
	// compares against.
	clock := time.Now()
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
	member := growMemberValue(docID, scanCode)
	if err := g.postings.SetAdd(diskKey(p, diskBlockMembers), [][]byte{member}); err != nil {
		return errors.Wrapf(err, "write doc %d to posting %d", docID, p)
	}
	if err := g.posts.Put(growDocKey(docID), growUint32Value(p)); err != nil {
		return errors.Wrap(err, "write the posting map")
	}
	routingValue := growCodeValue(routingCode)
	if err := g.routing.Put(growDocKey(docID), routingValue); err != nil {
		return errors.Wrap(err, "write a routing code")
	}
	// Its list, with the distances of its edges: a value of its own in the
	// store, or, with consolidated edges, waiting with the other changes of
	// its posting. The store of the edges counts what it writes.
	if err := g.edgeStore.add(p, docID, edges, edgeDists); err != nil {
		return err
	}
	raw := make([]byte, 4*len(vector))
	for i, x := range vector {
		putFloat32(raw[4*i:], x)
	}
	// At the position of its doc id, or, where the vectors have slots, in the
	// next one, which its table then tells.
	at := docID
	if g.meta.VectorGen > 0 {
		at = uint64(g.nextSlot)
	}
	if err := g.writeFile(growTargetVectors, raw, int64(at)*int64(len(raw))); err != nil {
		return errors.Wrapf(err, "write the vector of doc %d", docID)
	}
	if g.meta.VectorGen > 0 {
		if err := g.takeSlot(docID); err != nil {
			return err
		}
	}
	g.stats.MemberRow.add(1, len(member))
	g.stats.MemberMap.add(1, 4)
	g.stats.MemberRouting.add(1, len(routingValue))
	g.stats.MemberVector.add(1, len(raw))
	words := st.side.codeWords
	copy(st.side.codes[int(docID)*words:(int(docID)+1)*words], routingCode)
	st.side.postingOf[docID] = p
	g.live[docID] = true
	g.size[p]++
	g.meta.Live++
	// Its edges are those of the index as it is with it.
	if err := g.setBuilt(docID, g.held()); err != nil {
		return err
	}
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

	if g.consolidated() {
		// The rows of edges of the postings this insert changed, once they
		// have their share of changes. A posting that split has none left.
		clock = time.Now()
		if err := g.edgeStore.settle(); err != nil {
			return err
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

// begin starts a write of the index over what the given plan read: the write
// has a number, one more than the last, and the plan the number of the last
// write there was when it searched. What was written between the two is what
// the plan did not see.
func (g *growIndex) begin(plan *growPlan) {
	g.seq++
	g.sinceMerge++
	g.applying = plan.seq
	g.reread = g.reread[:0]
}

// outdated tells whether a vector left the index since the plan searched: its
// doc id may have been given again, to another vector, and what the plan
// holds of its neighbors and of their vectors may be of the one that left.
// Such a plan is made again.
func (g *growIndex) outdated(plan *growPlan) bool {
	return g.removedSeq > plan.seq
}

// heldNeighbors returns, of the neighbors a search found and their
// distances, those the index still holds: the same slices when it holds all.
func (g *growIndex) heldNeighbors(neighbors []uint64, dists []float32) ([]uint64, []float32) {
	all := true
	for _, u := range neighbors {
		if !g.holds(u) {
			all = false
			break
		}
	}
	if all {
		return neighbors, dists
	}
	held, heldDists := make([]uint64, 0, len(neighbors)), make([]float32, 0, len(dists))
	for i, u := range neighbors {
		if g.holds(u) {
			held, heldDists = append(held, u), append(heldDists, dists[i])
		}
	}
	return held, heldDists
}

// insertBatch adds the given vectors as insert adds one, with their searches
// run by the given number of goroutines at once (v5.2, bench-report.md,
// "inserts con búsquedas en paralelo"). Every search runs over the index as
// it is when the batch starts, while nobody writes: it does not see the
// vectors of its batch that come before it. Then the writes of every insert
// are applied one after another, in the order given, while nobody searches.
// What it leaves depends on the batches and not on the searchers: a batch of
// one vector is insert.
func (g *growIndex) insertBatch(ctx context.Context, docIDs []uint64, vectors [][]float32, searchers int) error {
	if len(docIDs) != len(vectors) {
		return errors.Errorf("%d doc ids for %d vectors", len(docIDs), len(vectors))
	}
	if searchers < 1 {
		return errors.Errorf("%d searchers", searchers)
	}
	for i, docID := range docIDs {
		if err := g.insertable(docID, vectors[i]); err != nil {
			return err
		}
		if slices.Contains(docIDs[:i], docID) {
			return errors.Errorf("doc %d is in the batch twice", docID)
		}
	}
	for len(g.batch) < len(docIDs) {
		g.batch = append(g.batch, newGrowPlan())
	}
	plans := g.batch[:len(docIDs)]

	// The searches, at once. Nothing of the index changes meanwhile.
	clock := time.Now()
	reads, bytes, err := g.counted()
	if err != nil {
		return err
	}
	if err := g.planBatch(ctx, plans, docIDs, vectors, searchers); err != nil {
		return err
	}
	readsAfter, bytesAfter, err := g.counted()
	if err != nil {
		return err
	}
	g.stats.StoreSearch.add(int(readsAfter-reads), int(bytesAfter-bytes))
	g.stats.SearchPhase += time.Since(clock)

	// The writes, in order. A plan is brought up to what the inserts of
	// its batch before it have changed.
	clock = time.Now()
	if g.batchDirty == nil {
		g.batchDirty = make(map[uint64]struct{})
	}
	clear(g.batchDirty)
	defer func() { g.batchDirty = nil }()
	g.batchApplied = g.batchApplied[:0]
	for _, plan := range plans {
		if err := g.refresh(ctx, plan); err != nil {
			return err
		}
		g.batchStale = false
		if err := g.apply(ctx, plan); err != nil {
			return err
		}
		if g.batchStale {
			g.stats.PlansStale++
		}
		if g.batchVerify {
			g.batchApplied = append(g.batchApplied, plan.docID)
		}
	}
	g.stats.Batches++
	g.stats.BatchInserts += len(plans)
	g.stats.ApplyPhase += time.Since(clock)
	return nil
}

// planBatch runs the searches of a batch into the given plans, with the given
// number of goroutines, each taking the next vector. The index must not
// change meanwhile. What every plan holds does not depend on how many
// goroutines there are: it is the search of its vector over the index as it
// is.
func (g *growIndex) planBatch(ctx context.Context, plans []*growPlan, docIDs []uint64, vectors [][]float32, searchers int,
) error {
	var next atomic.Int64
	eg := enterrors.NewErrorGroupWrapper(g.logger)
	for range min(searchers, len(docIDs)) {
		eg.Go(func() error {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(docIDs) {
					return nil
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				plans[i].work = nil
				if err := g.plan(ctx, plans[i], docIDs[i], vectors[i]); err != nil {
					return err
				}
			}
		})
	}
	return eg.Wait()
}

// refresh brings a plan of a batch up to what the inserts of the batch
// applied before it have changed: if one of them split the posting the plan
// chose, the posting is chosen again. The rest of a plan holds or is seen
// to when it is applied: its neighbors are doc ids, whose postings are
// looked up then; the vectors and the distances it read do not change; and
// a list of edges it kept that one of those inserts changed is read again
// when the inverse edges ask for it (listOf).
func (g *growIndex) refresh(ctx context.Context, plan *growPlan) error {
	if p := plan.entry[0]; p >= uint64(len(g.retired)) || g.retired[p] {
		entry, err := g.st.centroids.nearest(ctx, plan.vector, max(int(g.ix.entryPostings.Load()), 1))
		if err != nil {
			return err
		}
		if len(entry) == 0 || entry[0] >= uint64(len(g.retired)) || g.retired[entry[0]] {
			return errors.Errorf("the centroid graph returned no live posting for doc %d", plan.docID)
		}
		plan.entry = entry
		g.stats.PlansReassign++
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
		if read > 0 || !g.consolidated() {
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
		// With consolidated edges the new list waits in memory instead, with
		// its distances, until the row of its posting is written.
		if err := g.edgeStore.replace(q, u, edges, updated, updatedDists); err != nil {
			return err
		}
		// What the insert kept of the posting follows, should the same list
		// be looked at again. What the other plans of its batch kept of it
		// does not.
		copy(edges, updated)
		g.listSeq[q] = g.seq
		if g.batchDirty != nil {
			g.batchDirty[u] = struct{}{}
		}
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
	var rows [2][][]byte
	var halfIDs [2][]uint64
	var halfLists [2][][]uint32
	for i, docID := range dp.docIDs {
		c := sideOf[i]
		rows[c] = append(rows[c], growMemberValue(docID, dp.codes[i*words:(i+1)*words]))
		halfIDs[c], halfLists[c] = append(halfIDs[c], docID), append(halfLists[c], dp.edgesOfMember(i))
	}
	// The lists of the halves, as the store of the edges keeps them: a value
	// each in the row of edges of its half, or, with consolidated edges, the
	// rows of the halves written whole, with what waited of the posting in
	// them, and the row of the posting itself deleted.
	for c := range uint32(2) {
		if err := g.edgeStore.writePosting(first+c, halfIDs[c], halfLists[c], true); err != nil {
			return err
		}
	}
	if err := g.edgeStore.retire(p); err != nil {
		return err
	}
	for c := range uint32(2) {
		values := rows[c]
		if err := g.postings.SetAdd(diskKey(first+c, diskBlockMembers), values); err != nil {
			return errors.Wrapf(err, "write row %d of posting %d", diskBlockMembers, first+c)
		}
		bytes := 0
		for _, value := range values {
			bytes += len(value)
		}
		g.stats.SplitRowsW.add(len(values), bytes)
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
	if err := g.writeFile(growTargetCentroids, rawMeans, int64(first)*int64(4*st.dims)); err != nil {
		return errors.Wrap(err, "write the centroids of the halves")
	}
	g.stats.SplitCentroid.add(2, len(rawMeans))
	if err := g.graphChanges(); err != nil {
		return err
	}
	centroids := st.centroids
	centroids.vectors = append(centroids.vectors, means[0], means[1])
	centroids.publish()
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
	g.listSeq = append(g.listSeq, g.seq, g.seq)
	g.retired[p] = true
	g.retiredMu.Unlock()
	g.size = append(g.size, uint32(sizes[0]), uint32(sizes[1]))
	g.size[p] = 0
	g.meta.Postings += 2
	g.meta.Splits++
	g.stats.Splits++
	if g.meta.Postings+2 > g.scratchPostings {
		// The next split would give out postings the scratch of a search has
		// no room for.
		g.restate(st.side)
	}
	return nil
}

// edgesOfMember returns the edges of member i of a posting whose edges were
// joined.
func (dp *diskPosting) edgesOfMember(i int) []uint32 {
	return dp.edges[dp.edgeStart[i]:dp.edgeStart[i+1]]
}

// joinLists lays the edges of a fetched posting out by member, in the order
// of its members, from what fetchLists read of them.
func (g *growIndex) joinLists(p uint32, dp *diskPosting) error {
	return g.edgeStore.join(p, dp)
}

// distsOf returns the distances of the edges of a member of posting p. read
// is the bytes it read from the store for them.
func (g *growIndex) distsOf(p uint32, docID uint64) (dists []float32, read int, err error) {
	return g.edgeStore.distsOf(p, docID)
}

// storedDists returns the distances of the edges of a doc as their bucket
// holds them, and the bytes it read.
func (g *growIndex) storedDists(docID uint64) (dists []float32, read int, err error) {
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

// flushPending has the store of the edges write whatever it keeps waiting in
// memory. After it nothing waits: what the index is, is in its store.
func (g *growIndex) flushPending() error {
	return g.edgeStore.flush()
}
