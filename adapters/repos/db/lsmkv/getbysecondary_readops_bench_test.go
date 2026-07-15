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

package lsmkv

// Read-op-count microbenchmark harness for secondary-key (docID) resolution on a
// Replace bucket (gh#309). It is the MEASUREMENT INSTRUMENT built before any behavior
// change: it records the SERIAL baseline read-op count per phase (index / value /
// recheck) for resolving 500 random docIDs, and records the batched path the same way
// so the read-op reduction ratio can be compared across scales.
//
// Why a counting mock and not only diskio.MeteredReader:
// On current main, secondary/primary index descents (segmentindex.DiskTree.Get) are
// in-memory traversals over an mmap/heap byte slice; they issue no read syscalls and are
// invisible to diskio.MeteredReader. Only value reads (segment.copyNode) flow through
// diskio.MeteredReader (segment.go:719), and only in pread mode. So per-phase counts use
// a two-part scheme the AC explicitly allows ("via diskio.MeteredReader observer or a
// counting mock"):
//   - VALUE phase: read through a real diskio.MeteredReader with a counting callback
//     (mirrors segment.go:719).
//   - INDEX + RECHECK phases: a counting DiskTree traversal (the counting mock) that
//     ports segmentindex.DiskTree.Get's node-walk and counts node-reads. Trees are
//     reconstructed from the flushed segment files via public segmentindex APIs, so no
//     production code changes.
//
// Fidelity is pinned two ways: the counting traversal's returned Node is asserted equal
// to the real DiskTree.Get for every probed key, and the resolver's resolved value is
// asserted equal to the production bucket.GetBySecondary for all 500 keys. If the counting
// mock ever drifts from the real serial path, these assertions fail loudly.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// readOpsScaleFlag selects the harness scale. "scaled" is the CI-viable default; "full"
// (>=1M keys/segment) is the dev-box run behind the -scale=full path referenced by the AC.
// The read-op-count RATIO is scale-representative (descent depth is log N; the ratio is
// stable), so CI runs the scaled shape and one recorded full-scale run pins the ratio.
var readOpsScaleFlag = flag.String("lsmkv.readops.scale", "scaled",
	"read-op harness scale: 'scaled' (CI default) or 'full' (>=1M keys/segment, dev box)")

const secondaryPos = 0

// readOpsScale is the parameterized shape per the design (§ Test/benchmark strategy):
// segments 8-13, keys-per-segment, value sizes 1.5-6.5KB, resolving 500 random docIDs.
type readOpsScale struct {
	name         string
	segments     int
	keysPerSeg   int
	valueSizeMin int // 1.5KB
	valueSizeMax int // 6.5KB
	numResolve   int
	useBloom     bool
	seed         int64
}

func scaledShape() readOpsScale {
	return readOpsScale{
		name:         "scaled-ci",
		segments:     13, // matches the LTK profiled hot shard (8-13 range)
		keysPerSeg:   200,
		valueSizeMin: 1536, // 1.5 KiB
		valueSizeMax: 6656, // 6.5 KiB
		numResolve:   500,
		useBloom:     false, // deterministic, matchable index-read counts; bloom-on is a noted dimension
		seed:         42,
	}
}

func fullShape() readOpsScale {
	s := scaledShape()
	s.name = "full-scale"
	s.keysPerSeg = 1_000_000 // >=1M keys/segment per the AC; dev-box only
	return s
}

// phaseCounters accumulates read-ops per phase over a whole 500-key resolution.
// index/recheck read-ops are DiskTree node-reads (page touches: the metric where the
// design's "not 500 x log N" reduction would appear); value read-ops are metered reads.
// GetCalls are recorded alongside as a coarser cross-check.
type phaseCounters struct {
	indexNodeReads   int64
	indexGetCalls    int64
	valueReadOps     int64
	valueBytes       int64
	recheckNodeReads int64
	recheckGetCalls  int64
}

func (p *phaseCounters) add(o phaseCounters) {
	p.indexNodeReads += o.indexNodeReads
	p.indexGetCalls += o.indexGetCalls
	p.valueReadOps += o.valueReadOps
	p.valueBytes += o.valueBytes
	p.recheckNodeReads += o.recheckNodeReads
	p.recheckGetCalls += o.recheckGetCalls
}

// reconstructedSegment holds one segment's public DiskTrees plus its raw contents, in
// newest-first order (index 0 = newest), reconstructed from a flushed .db file.
type reconstructedSegment struct {
	contents  []byte
	secondary *segmentindex.DiskTree
	primary   *segmentindex.DiskTree
	secData   []byte // raw secondary index bytes (for the counting traversal)
	priData   []byte // raw primary index bytes (for the counting traversal)
}

// artifact is the fixed schema recorded for both scales so a future reviewer (and the
// batch-vs-serial ratio gate) can compare like-for-like.
type artifact struct {
	Scale            string `json:"scale"`
	GOOS             string `json:"goos"`
	GOARCH           string `json:"goarch"`
	NumCPU           int    `json:"num_cpu"`
	Segments         int    `json:"segments"`
	KeysPerSegment   int    `json:"keys_per_segment"`
	ValueSizeMin     int    `json:"value_size_min_bytes"`
	ValueSizeMax     int    `json:"value_size_max_bytes"`
	KeysResolved     int    `json:"keys_resolved"`
	Path             string `json:"path"` // "serial" | "batch"
	IndexNodeReads   int64  `json:"index_node_reads"`
	IndexGetCalls    int64  `json:"index_get_calls"`
	ValueReadOps     int64  `json:"value_read_ops"`
	ValueBytes       int64  `json:"value_bytes"`
	RecheckNodeReads int64  `json:"recheck_node_reads"`
	RecheckGetCalls  int64  `json:"recheck_get_calls"`
	// ArenaBytes is the peak resident phase-2 arena size for the batch path (memory-axis
	// observability): one alloc sized sum(node.End-node.Start), bounded ~3.3MB by the
	// 500-key view-hold cap.
	ArenaBytes int64     `json:"arena_bytes"`
	WallNanos  int64     `json:"wall_nanos"`
	RecordedAt time.Time `json:"recorded_at"`
}

// TestBucketGetBySecondaryReadOpsBaseline records the serial read-op baseline for the
// current shape (scaled by default; full via -lsmkv.readops.scale=full), validates the
// counting mock against the real serial path, and scaffolds the ratio gate. Named with
// the TestBucket prefix so it is included in the run-TestBucket gate:
//
//	go test -count=1 -run TestBucket ./adapters/repos/db/lsmkv/
func TestBucketGetBySecondaryReadOpsBaseline(t *testing.T) {
	shape := scaledShape()
	if *readOpsScaleFlag == "full" {
		shape = fullShape()
		if testing.Short() {
			t.Skip("full-scale (>=1M keys/segment) skipped under -short")
		}
	}

	art, serial, batch := recordSerialBaseline(t, shape)

	// three phases are populated: index/recheck via the counting mock, value via the
	// real diskio.MeteredReader. On the clean baseline shape (no updates, no tombstones)
	// every resolved key is a live hit, so value read-ops == keys resolved exactly.
	require.Positive(t, serial.indexNodeReads, "index phase must record node-reads")
	require.Positive(t, serial.recheckNodeReads, "recheck phase must record node-reads (newer-segment primary descents)")
	require.EqualValues(t, shape.numResolve, serial.valueReadOps,
		"value phase: one metered read per live hit")
	require.Positive(t, art.ArenaBytes, "batch path must record a non-zero peak resident arena size")

	writeAndLogArtifact(t, art)
	assertRatioGate(t, shape, serial, batch)
}

// recordSerialBaseline builds the parameterized bucket, resolves numResolve random docIDs
// through the REAL serial bucket.GetBySecondary (ground truth) AND through the counting
// resolver (the instrument), asserts they agree, and returns the recorded artifact + counts.
func recordSerialBaseline(t *testing.T, shape readOpsScale) (artifact, phaseCounters, phaseCounters) {
	t.Helper()
	ctx := context.Background()

	bucket, dir, docIDs := buildReadOpsBucket(t, shape)

	rng := rand.New(rand.NewSource(shape.seed + 1))
	targets := pickRandom(rng, docIDs, shape.numResolve)
	targetKeys := make([][]byte, len(targets))
	for i, docID := range targets {
		targetKeys[i] = encodeDocID(docID)
	}

	segs := reconstructSegments(t, dir)
	require.Len(t, segs, shape.segments, "expected one reconstructed segment per flush")

	var total phaseCounters
	start := time.Now()
	for i, docID := range targets {
		key := targetKeys[i]

		// Ground truth from the production serial path.
		wantVal, wantErr := bucket.GetBySecondary(ctx, secondaryPos, key)

		// Instrument: same walk over the reconstructed trees, counting per phase.
		gotVal, counters, gotErr := resolveSerialCounting(t, segs, key)
		total.add(counters)

		requireSameResult(t, docID, wantVal, wantErr, gotVal, gotErr)
	}
	wall := time.Since(start)

	// Batch per-phase counts in the SAME session/box (so the ratio is like-for-like),
	// via the faithful batch-algorithm mirror over the same reconstructed trees.
	_, batch := resolveBatchCounting(t, segs, targetKeys)

	// Peak resident arena bytes from the REAL batch path (memory observability).
	arenaBytes := measureBatchArenaBytes(t, bucket, targetKeys)

	art := artifact{
		Scale:            shape.name,
		GOOS:             runtime.GOOS,
		GOARCH:           runtime.GOARCH,
		NumCPU:           runtime.NumCPU(),
		Segments:         shape.segments,
		KeysPerSegment:   shape.keysPerSeg,
		ValueSizeMin:     shape.valueSizeMin,
		ValueSizeMax:     shape.valueSizeMax,
		KeysResolved:     shape.numResolve,
		Path:             "serial",
		IndexNodeReads:   total.indexNodeReads,
		IndexGetCalls:    total.indexGetCalls,
		ValueReadOps:     total.valueReadOps,
		ValueBytes:       total.valueBytes,
		RecheckNodeReads: total.recheckNodeReads,
		RecheckGetCalls:  total.recheckGetCalls,
		ArenaBytes:       int64(arenaBytes),
		WallNanos:        wall.Nanoseconds(),
		RecordedAt:       time.Now().UTC(),
	}
	return art, total, batch
}

// measureBatchArenaBytes runs the REAL batch phase-1/phase-2 over the target keys and
// returns the arena size the production path allocated (sum(node.End-node.Start) over
// all phase-1 hits) -- the peak resident arena bytes for the memory-axis AC.
func measureBatchArenaBytes(t *testing.T, bucket *Bucket, keys [][]byte) int {
	t.Helper()
	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	unresolved := make([]secondaryBatchKey, len(keys))
	for i, k := range keys {
		unresolved[i] = secondaryBatchKey{origIdx: i, key: k}
	}
	hits, err := bucket.disk.getBySecondaryBatchIndexHits(context.Background(), secondaryPos, unresolved, view.Disk, nil)
	require.NoError(t, err)
	_, arenaBytes, err := bucket.disk.readSecondaryBatchValuesConcurrent(
		context.Background(), hits, view.Disk, defaultSecondaryBatchReadConcurrency, nil,
	)
	require.NoError(t, err)
	return arenaBytes
}

// resolveSerialCounting mirrors the production serial resolution
// (segment_group.go getBySecondaryWithSegmentList + existsWithConsistentViewUpTo) over the
// reconstructed newest-first segment list, counting read-ops per phase. Bloom is disabled to
// match the bucket's WithUseBloomFilter(false), so index Gets happen on every segment until
// the first confirmed hit, exactly as the real path does with bloom off.
func resolveSerialCounting(t *testing.T, segs []reconstructedSegment, key []byte) ([]byte, phaseCounters, error) {
	t.Helper()
	var c phaseCounters

	for i := range segs { // newest -> oldest
		seg := segs[i]

		// INDEX phase: confirmed secondary descent.
		node, nodeReads, err := treeGetCounting(seg.secData, key)
		c.indexNodeReads += int64(nodeReads)
		c.indexGetCalls++
		assertCountingMatchesReal(t, seg.secondary, key, node, err)
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				continue // key not in this segment; walk older
			}
			return nil, c, err
		}

		// VALUE phase: read the node's bytes through a real diskio.MeteredReader.
		raw, err := readValueMetered(seg.contents, node, &c)
		if err != nil {
			return nil, c, err
		}
		primaryKey, value, err := parseReplaceValue(raw)
		if errors.Is(err, entlsmkv.Deleted) {
			return nil, c, entlsmkv.Deleted // tombstone wins; older segments not consulted
		}
		if err != nil {
			return nil, c, err
		}

		// RECHECK phase: is there a NEWER version of this primary key? Descend the primary
		// index of every segment newer than the hit (indices [0, i)). Any hit => stale.
		for j := 0; j < i; j++ {
			pnode, pNodeReads, perr := treeGetCounting(segs[j].priData, primaryKey)
			c.recheckNodeReads += int64(pNodeReads)
			c.recheckGetCalls++
			assertCountingMatchesReal(t, segs[j].primary, primaryKey, pnode, perr)
			if perr == nil {
				return nil, c, entlsmkv.NotFound // newer version exists; this entry is stale
			}
			if !errors.Is(perr, entlsmkv.NotFound) {
				return nil, c, perr
			}
		}
		return value, c, nil
	}
	return nil, c, entlsmkv.NotFound
}

// treeGetCounting ports segmentindex.DiskTree.Get's node-walk and counts each node visited
// (one node-read per iteration). This is the counting mock for the in-memory index phases.
// It reads the same on-disk DiskTree bytes the production tree uses, so the count reflects
// the real descent shape.
func treeGetCounting(data, key []byte) (segmentindex.Node, int, error) {
	var out segmentindex.Node
	if len(data) == 0 {
		return out, 0, entlsmkv.NotFound
	}
	rw := byteops.NewReadWriter(data)
	keyBuf := make([]byte, len(key))
	nodeReads := 0
	for {
		if rw.Position+4 > uint64(len(data)) || rw.Position+4 < 4 {
			return out, nodeReads, entlsmkv.NotFound
		}
		nodeReads++ // one node touched this iteration

		keyLen := rw.ReadUint32()
		if int(keyLen) > len(keyBuf) {
			keyBuf = make([]byte, int(keyLen))
		} else if int(keyLen) < len(keyBuf) {
			keyBuf = keyBuf[:keyLen]
		}
		if _, err := rw.CopyBytesFromBuffer(uint64(keyLen), keyBuf); err != nil {
			return out, nodeReads, fmt.Errorf("copy node key: %w", err)
		}

		switch cmp := bytes.Compare(key, keyBuf); {
		case cmp == 0:
			out.Key = keyBuf
			out.Start = rw.ReadUint64()
			out.End = rw.ReadUint64()
			return out, nodeReads, nil
		case cmp < 0:
			rw.MoveBufferPositionForward(2 * 8)
			rw.Position = rw.ReadUint64() // left child
		default:
			rw.MoveBufferPositionForward(3 * 8)
			rw.Position = rw.ReadUint64() // right child
		}
	}
}

// assertCountingMatchesReal pins the counting mock to the production DiskTree.Get: the Node
// (or NotFound) returned by the ported traversal must equal the real tree's answer.
func assertCountingMatchesReal(t *testing.T, tree *segmentindex.DiskTree, key []byte,
	gotNode segmentindex.Node, gotErr error,
) {
	t.Helper()
	realNode, realErr := tree.Get(key)
	if gotErr != nil || realErr != nil {
		require.Equal(t, realErr != nil, gotErr != nil,
			"counting traversal and DiskTree.Get disagree on found/not-found for key %x", key)
		return
	}
	require.Equal(t, realNode.Start, gotNode.Start, "counting traversal node.Start drift for key %x", key)
	require.Equal(t, realNode.End, gotNode.End, "counting traversal node.End drift for key %x", key)
}

// readValueMetered reads the node's byte range through a real diskio.MeteredReader so the
// value phase is counted via the production metering type (mirrors segment.go:719). The
// contents slice already holds the segment bytes; the point is to route the read through
// MeteredReader and count via its callback, cache-independently.
func readValueMetered(contents []byte, node segmentindex.Node, c *phaseCounters) ([]byte, error) {
	if node.End < node.Start || node.End > uint64(len(contents)) {
		return nil, fmt.Errorf("node range [%d:%d] out of bounds (len %d)", node.Start, node.End, len(contents))
	}
	buf := make([]byte, node.End-node.Start)
	mr := diskio.NewMeteredReader(newBytesReaderAt(contents), func(read int64, _ int64) {
		c.valueReadOps++
		c.valueBytes += read
	})
	if _, err := mr.ReadAt(buf, int64(node.Start)); err != nil {
		return nil, fmt.Errorf("metered value read: %w", err)
	}
	return buf, nil
}

// parseReplaceValue mirrors segment_replace_strategy.go replaceStratParseData: byte 0
// tombstone flag, [1:9] value length LE, value, then pk length + primary key.
func parseReplaceValue(in []byte) (primaryKey, value []byte, err error) {
	if len(in) == 0 {
		return nil, nil, entlsmkv.NotFound
	}
	if in[0] == 0x01 {
		return nil, nil, entlsmkv.Deleted
	}
	if len(in) < 9 {
		return nil, nil, fmt.Errorf("replace value too short: %d", len(in))
	}
	valueLength := binary.LittleEndian.Uint64(in[1:9])
	pkStart := 9 + valueLength
	if pkStart+4 > uint64(len(in)) {
		return nil, nil, fmt.Errorf("replace value truncated before pk length")
	}
	pkLength := binary.LittleEndian.Uint32(in[pkStart:])
	pkBytesStart := pkStart + 4
	if pkBytesStart+uint64(pkLength) > uint64(len(in)) {
		return nil, nil, fmt.Errorf("replace value truncated in pk")
	}
	return in[pkBytesStart : pkBytesStart+uint64(pkLength)], in[9 : 9+valueLength], nil
}

// ---- bucket construction ------------------------------------------------------------

// buildReadOpsBucket writes `segments` flushed Replace segments, each carrying keysPerSeg
// distinct docIDs (disjoint per segment so a docID lives in exactly one segment). Compaction
// is disabled so the on-disk segment count equals the flush count and ordering is stable.
// Returns the bucket, its dir, and the universe of resolvable docIDs.
func buildReadOpsBucket(tb testing.TB, shape readOpsScale) (*Bucket, string, []uint64) {
	tb.Helper()
	ctx := context.Background()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(
		ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithPread(true),
		WithMinMMapSize(0), // force the pread value path (production LTK shape)
		WithUseBloomFilter(shape.useBloom),
		WithDisableCompaction(true),
	)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, bucket.Shutdown(context.Background())) })

	rng := rand.New(rand.NewSource(shape.seed))
	valueBuf := make([]byte, shape.valueSizeMax)
	docIDs := make([]uint64, 0, shape.segments*shape.keysPerSeg)

	var docID uint64
	for s := 0; s < shape.segments; s++ {
		for k := 0; k < shape.keysPerSeg; k++ {
			secKey := encodeDocID(docID)
			priKey := encodePrimaryKey(docID)
			size := shape.valueSizeMin + rng.Intn(shape.valueSizeMax-shape.valueSizeMin+1)
			val := valueBuf[:size]
			rng.Read(val)
			require.NoError(tb, bucket.Put(priKey, val, WithSecondaryKey(secondaryPos, secKey)))
			docIDs = append(docIDs, docID)
			docID++
		}
		require.NoError(tb, bucket.FlushAndSwitch())
	}
	return bucket, dir, docIDs
}

// reconstructSegments loads every flushed .db segment file and rebuilds its public
// DiskTrees, newest-first (index 0 = newest). Segment files are segment-<unixnanos>.db, so
// descending filename order is newest-first.
func reconstructSegments(tb testing.TB, dir string) []reconstructedSegment {
	tb.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "segment-*.db"))
	require.NoError(tb, err)
	require.NotEmpty(tb, matches)
	sort.Sort(sort.Reverse(sort.StringSlice(matches))) // newest first

	segs := make([]reconstructedSegment, 0, len(matches))
	for _, path := range matches {
		contents, err := os.ReadFile(path)
		require.NoError(tb, err)
		header, err := segmentindex.ParseHeader(contents[:segmentindex.HeaderSize])
		require.NoError(tb, err)

		priData, err := header.PrimaryIndex(contents)
		require.NoError(tb, err)
		secData, err := header.SecondaryIndex(contents, secondaryPos)
		require.NoError(tb, err)

		segs = append(segs, reconstructedSegment{
			contents:  contents,
			secData:   secData,
			priData:   priData,
			secondary: segmentindex.NewDiskTree(secData),
			primary:   segmentindex.NewDiskTree(priData),
		})
	}
	return segs
}

// ---- read-op ratio-consistency gate scaffold ----------------------------------------

// assertRatioGate fires the read-op ratio gate with REAL batch counts: it pins
// value_reads == live hits and records the index-read reduction ratio for the current
// scale. The ratio is expected ~1.0 (batch-NEUTRAL): sorted DiskTree.Get has no cursor
// amortization, so the batch issues the same node-reads as serial; the leverage is
// wall-time/concurrency, gated separately by the phase-2 concurrency-effectiveness
// tests, not by read-op count. The cross-scale +/-15% consistency (scaled vs recorded
// full-scale) is pinned by the artifact ratios and the reductionRatiosWithinTolerance
// math (TestReadOpsRatioToleranceMath).
func assertRatioGate(t *testing.T, shape readOpsScale, serial, batch phaseCounters) {
	t.Helper()
	require.Equal(t, serial.valueReadOps, batch.valueReadOps,
		"value read-op count must be batch-neutral (one read per live hit)")
	ratio := reductionRatio(serial, batch)
	require.Positive(t, ratio, "index-read reduction ratio must be computable (batch index reads > 0)")
	t.Logf("read-op ratio gate (%s): index reduction serial/batch = %.4f | index serial=%d batch=%d | "+
		"value serial=%d batch=%d | recheck serial=%d batch=%d",
		shape.name, ratio, serial.indexNodeReads, batch.indexNodeReads,
		serial.valueReadOps, batch.valueReadOps, serial.recheckNodeReads, batch.recheckNodeReads)
}

// reductionRatio is the index-read-count reduction ratio (serial / batch). >1 means the
// batch path issues fewer index reads. Guarded against divide-by-zero.
func reductionRatio(serial, batch phaseCounters) float64 {
	if batch.indexNodeReads == 0 {
		return 0
	}
	return float64(serial.indexNodeReads) / float64(batch.indexNodeReads)
}

// reductionRatiosWithinTolerance implements the scale-consistency assertion: the
// full-scale reduction ratio must match the scaled-CI ratio within +/-15%. It is used by
// the cross-scale gate once both scales carry batch counts. Exposed as a pure function so
// it is unit-checkable.
func reductionRatiosWithinTolerance(scaledRatio, fullRatio, tol float64) bool {
	if scaledRatio == 0 {
		return false
	}
	rel := (fullRatio - scaledRatio) / scaledRatio
	if rel < 0 {
		rel = -rel
	}
	return rel <= tol
}

// TestReadOpsRatioToleranceMath pins the +/-15% band math so the ratio gate behaves as
// specified when batch counts arrive.
func TestReadOpsRatioToleranceMath(t *testing.T) {
	require.True(t, reductionRatiosWithinTolerance(10.0, 11.0, 0.15))  // +10%
	require.True(t, reductionRatiosWithinTolerance(10.0, 8.5, 0.15))   // -15% exactly
	require.False(t, reductionRatiosWithinTolerance(10.0, 12.0, 0.15)) // +20% -> falsifies scale-representativeness
	require.False(t, reductionRatiosWithinTolerance(0, 1, 0.15))       // undefined baseline
}

// ---- artifact ----------------------------------------------------------------------

func writeAndLogArtifact(t *testing.T, art artifact) {
	t.Helper()
	blob, err := json.MarshalIndent(art, "", "  ")
	require.NoError(t, err)
	out := filepath.Join(os.TempDir(), fmt.Sprintf("lsmkv-readops-artifact-%s-%d.json", art.Scale, time.Now().UnixNano()))
	require.NoError(t, os.WriteFile(out, blob, 0o600))
	art.Path = out
	t.Logf("read-op baseline artifact (%s) written to %s\n%s", art.Scale, out, string(blob))
}

// ---- benchmark wrapper -------------------------------------------------------------

// BenchmarkGetBySecondaryReadOps reports the serial per-phase read-op counts as custom
// benchmark metrics (per resolved key), so `go test -bench` surfaces the baseline in a
// machine-readable form alongside the recorded artifact.
func BenchmarkGetBySecondaryReadOps(b *testing.B) {
	shape := scaledShape()
	if *readOpsScaleFlag == "full" {
		shape = fullShape()
	}
	bucket, dir, docIDs := buildReadOpsBucket(b, shape)
	defer bucket.Shutdown(context.Background())
	segs := reconstructSegments(b, dir)
	rng := rand.New(rand.NewSource(shape.seed + 7))
	targets := pickRandom(rng, docIDs, shape.numResolve)

	b.ResetTimer()
	var last phaseCounters
	for i := 0; i < b.N; i++ {
		var total phaseCounters
		for _, docID := range targets {
			_, c, _ := resolveSerialCountingBench(segs, encodeDocID(docID))
			total.add(c)
		}
		last = total
	}
	b.StopTimer()
	denom := float64(shape.numResolve)
	b.ReportMetric(float64(last.indexNodeReads)/denom, "index_reads/key")
	b.ReportMetric(float64(last.valueReadOps)/denom, "value_reads/key")
	b.ReportMetric(float64(last.recheckNodeReads)/denom, "recheck_reads/key")
}

// ---- small helpers -----------------------------------------------------------------

func encodeDocID(docID uint64) []byte {
	// docID secondary key is little-endian (matches upsertObjectDataLSM in shard_write_put.go).
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], docID)
	return b[:]
}

func encodePrimaryKey(docID uint64) []byte {
	// 16-byte UUID-shaped primary key, unique per docID.
	var b [16]byte
	binary.BigEndian.PutUint64(b[8:], docID)
	return b[:]
}

func pickRandom(rng *rand.Rand, universe []uint64, n int) []uint64 {
	if n > len(universe) {
		n = len(universe)
	}
	idx := rng.Perm(len(universe))[:n]
	out := make([]uint64, n)
	for i, j := range idx {
		out[i] = universe[j]
	}
	return out
}

func requireSameResult(t *testing.T, docID uint64, wantVal []byte, wantErr error, gotVal []byte, gotErr error) {
	t.Helper()
	if wantErr != nil || gotErr != nil {
		require.Equal(t, wantErr != nil, gotErr != nil,
			"serial-path vs counting-resolver found/not-found disagreement for docID %d (want %v, got %v)",
			docID, wantErr, gotErr)
		return
	}
	require.Equal(t, wantVal, gotVal, "counting resolver value drift vs real GetBySecondary for docID %d", docID)
}

// bytesReaderAt adapts a []byte to diskio.Reader (Read + ReadAt) so the value phase can be
// read through a real diskio.MeteredReader.
type bytesReaderAt struct {
	data []byte
	pos  int
}

func newBytesReaderAt(data []byte) *bytesReaderAt { return &bytesReaderAt{data: data} }

func (r *bytesReaderAt) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("eof")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off > int64(len(r.data)) {
		return 0, fmt.Errorf("readat out of range")
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, fmt.Errorf("short read")
	}
	return n, nil
}

// resolveSerialCountingBench is the assertion-free variant used inside the benchmark loop
// (no per-key equality checks in the hot loop; correctness is gated by the Test).
func resolveSerialCountingBench(segs []reconstructedSegment, key []byte) ([]byte, phaseCounters, error) {
	var c phaseCounters
	for i := range segs {
		seg := segs[i]
		node, nodeReads, err := treeGetCounting(seg.secData, key)
		c.indexNodeReads += int64(nodeReads)
		c.indexGetCalls++
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				continue
			}
			return nil, c, err
		}
		raw, err := readValueMetered(seg.contents, node, &c)
		if err != nil {
			return nil, c, err
		}
		primaryKey, value, err := parseReplaceValue(raw)
		if errors.Is(err, entlsmkv.Deleted) {
			return nil, c, entlsmkv.Deleted
		}
		if err != nil {
			return nil, c, err
		}
		for j := 0; j < i; j++ {
			_, pNodeReads, perr := treeGetCounting(segs[j].priData, primaryKey)
			c.recheckNodeReads += int64(pNodeReads)
			c.recheckGetCalls++
			if perr == nil {
				return nil, c, entlsmkv.NotFound
			}
			if !errors.Is(perr, entlsmkv.NotFound) {
				return nil, c, perr
			}
		}
		return value, c, nil
	}
	return nil, c, entlsmkv.NotFound
}
