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

package compact

// Memory-peak benchmarks for the compactv2 pipeline.
//
// Each scenario exercises a specific memory hotspot in the new disk-based
// compaction path (see PR #9988). The scenarios are chosen to stress the
// in-memory state that survives the streaming-merge abstraction:
//
//   - SnapshotWriter.nodes is indexed by node ID, so peak grows with maxID,
//     not with live-node count (Sparse_*).
//   - InMemoryReader.Do builds a full DeserializationResult per file
//     (RawFileConversion_*).
//   - NWayMerger's per-node commitMerger holds linksPerLevel/pendingLinks
//     while a node is active across N iterators (FiveWayMerge_*,
//     HighFanout_*).
//   - SnapshotReader allocates a fresh DeserializationResult and uses 8
//     concurrent goroutines, each holding a 4MB block (SnapshotRead_*).
//   - Tombstone/LinksReplaced maps grow with churn, not live set
//     (TombstoneChurn_*).
//
// Each Bench* exposes a *PeakMB* / *HeapDeltaMB* / *NumGC* custom metric so
// `benchstat` can produce an A/B table against `main`. Sizes are tuned to
// run on a laptop in a few seconds at -benchtime=1x; bump the constants for
// adversarial sweeps.

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

// memProbe samples runtime.MemStats.HeapInuse in the background and reports
// peak heap + GC delta on Stop().
//
// The sampler is intentionally simple: a goroutine that reads MemStats every
// `interval` and tracks the max. We don't try to be cheap — these are
// benchmarks, not the prod hot path. Resolution is bounded by `interval`;
// short bursts shorter than that can be missed, so callers running tiny
// workloads should keep the interval at <=10ms.
type memProbe struct {
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	peak     atomic.Uint64
	startGC  uint32
	endGC    uint32
	startSys uint64
	endSys   uint64
}

func startMemProbe(interval time.Duration) *memProbe {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	p := &memProbe{
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		startGC:  m.NumGC,
		startSys: m.HeapInuse,
	}
	p.peak.Store(m.HeapInuse)

	go func() {
		defer close(p.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var s runtime.MemStats
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&s)
				if s.HeapInuse > p.peak.Load() {
					p.peak.Store(s.HeapInuse)
				}
			}
		}
	}()
	return p
}

func (p *memProbe) Stop() {
	p.stopOnce.Do(func() {
		close(p.stop)
		<-p.done
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapInuse > p.peak.Load() {
			p.peak.Store(m.HeapInuse)
		}
		p.endGC = m.NumGC
		p.endSys = m.HeapInuse
	})
}

func (p *memProbe) report(b *testing.B) {
	b.Helper()
	const MiB = 1024.0 * 1024.0
	b.ReportMetric(float64(p.peak.Load())/MiB, "PeakMB")
	// Heap delta = HeapInuse at end minus HeapInuse at start, post-GC.
	// Useful to detect leaks that survive the benchmark vs. peaks that
	// recover.
	delta := int64(p.endSys) - int64(p.startSys)
	b.ReportMetric(float64(delta)/MiB, "HeapDeltaMB")
	b.ReportMetric(float64(p.endGC-p.startGC), "NumGC")
}

// =============================================================================
// Scenario S1: Sparse-ID snapshot.
//
// 100k live nodes scattered across a 1e9 ID space. SnapshotWriter.nodes is
// indexed by node ID — see snapshot_writer.go:188-213 (ensureNodesCapacity).
// Peak must be dominated by the slice of nil *nodeState pointers, ~8 bytes
// each * maxID. For maxID=1e9 the slice header alone is ~8 GB of nil
// pointers, which is the worst case we care about. We cap the scenario at
// 1e7 by default to keep CI tractable; the SparseLarge variant bumps it.
// =============================================================================

func benchmarkSparseSnapshot(b *testing.B, liveNodes int, idSpread uint64) {
	b.Helper()
	r := rand.New(rand.NewSource(int64(liveNodes) ^ int64(idSpread)))

	// Pre-pick node IDs so generation cost is excluded from the measurement.
	ids := make([]uint64, liveNodes)
	for i := range ids {
		ids[i] = uint64(r.Int63n(int64(idSpread)))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		probe := startMemProbe(10 * time.Millisecond)

		w := NewSnapshotWriter(io.Discard)
		w.SetEntrypoint(ids[0], 1)
		// AddNode in arbitrary order; SnapshotWriter expects ascending
		// nodeID — we sort the ids slice once before iteration so the
		// allocation pattern matches what compactor.go produces.
		sortedIDs := make([]uint64, len(ids))
		copy(sortedIDs, ids)
		// Simple in-place insertion sort would be O(n^2); use slices.Sort
		// equivalent via the stdlib sort. We pull it in lazily to avoid
		// inflating the bench prologue.
		for i := 1; i < len(sortedIDs); i++ {
			for j := i; j > 0 && sortedIDs[j-1] > sortedIDs[j]; j-- {
				sortedIDs[j-1], sortedIDs[j] = sortedIDs[j], sortedIDs[j-1]
			}
		}
		for _, id := range sortedIDs {
			w.AddNode(id, 0, [][]uint64{{}}, false)
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}

		probe.Stop()
		probe.report(b)
	}
}

func BenchmarkMemorySparseSnapshot_100k_in_1e7(b *testing.B) {
	benchmarkSparseSnapshot(b, 100_000, 10_000_000)
}

func BenchmarkMemorySparseSnapshot_100k_in_1e8(b *testing.B) {
	benchmarkSparseSnapshot(b, 100_000, 100_000_000)
}

// Largest sparse-ID case — 100k live nodes spread across a 1e9 ID space.
// This is the shape we expect after very heavy churn (a billion historical
// inserts, most deleted, current live set ~100k). The expected peak per the
// 1e7/1e8 trajectory is ~16-18 GB; skip in -short mode and only run when
// the operator wants to confirm the upper bound.
func BenchmarkMemorySparseSnapshot_100k_in_1e9(b *testing.B) {
	if testing.Short() {
		b.Skip("multi-GB peak; run explicitly without -short")
	}
	benchmarkSparseSnapshot(b, 100_000, 1_000_000_000)
}

// =============================================================================
// Scenario S2: Dense snapshot round-trip.
//
// 1M live nodes, dense IDs, M=32. Exercises SnapshotWriter + SnapshotReader on
// the shape we'd see for a healthy graph. The Reader uses 8 concurrent
// goroutines pulling 4MB blocks (snapshot_reader.go:38) — peak should sit at
// roughly snapshotConcurrency * blockSize + the materialized
// DeserializationResult.
// =============================================================================

func benchmarkDenseSnapshotRoundtrip(b *testing.B, n int, m int) {
	b.Helper()

	dir := b.TempDir()
	snapPath := filepath.Join(dir, "dense.snapshot")

	// One-time setup outside the timed region: build the snapshot.
	f, err := os.Create(snapPath)
	if err != nil {
		b.Fatal(err)
	}
	w := NewSnapshotWriter(f)
	w.SetEntrypoint(0, 1)
	r := rand.New(rand.NewSource(42))
	for id := uint64(0); id < uint64(n); id++ {
		conns := make([]uint64, m)
		for j := range conns {
			conns[j] = uint64(r.Intn(n))
		}
		w.AddNode(id, 0, [][]uint64{conns}, false)
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
	f.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		probe := startMemProbe(10 * time.Millisecond)

		reader := NewSnapshotReader(testLogger())
		res, err := reader.ReadFromFile(snapPath)
		if err != nil {
			b.Fatal(err)
		}
		// Anchor `res` so it isn't GC'd before we measure peak.
		if len(res.Graph.Nodes) == 0 {
			b.Fatal("empty graph")
		}

		probe.Stop()
		probe.report(b)
	}
}

func BenchmarkMemoryDenseSnapshot_1M_M32(b *testing.B) {
	benchmarkDenseSnapshotRoundtrip(b, 1_000_000, 32)
}

// =============================================================================
// Scenario S3: High-fanout merge.
//
// Two sorted files with the same 50k node IDs, each node carrying maximum
// connections at level 0. NWayMerger's commitMerger accumulates
// linksPerLevel + pendingLinks for the current node across all iterators
// (nway_merger.go:264-276). Peak is dominated by holding both inputs'
// link slices for the active node + the heap.
//
// 4096 is the in-memory hard cap (maxConnectionsPerNodeInMemory). We push to
// 2048 to keep the file sizes reasonable while still stressing the merger.
// =============================================================================

func benchmarkHighFanoutMerge(b *testing.B, n int, linksPerNode int, files int) {
	b.Helper()

	dir := b.TempDir()
	logger := testLogger()
	paths := make([]string, files)

	for k := 0; k < files; k++ {
		path := filepath.Join(dir, fmt.Sprintf("hf_%d.sorted", k))
		paths[k] = path

		r := rand.New(rand.NewSource(int64(k) + 1))

		// Build a DeserializationResult directly — faster than writing then
		// re-reading a raw file, and we want the merger to be the bench's
		// hot path, not the WAL writer.
		res := ent.NewDeserializationResult(n)
		res.Graph.EntrypointChanged = true
		res.Graph.Entrypoint = 0
		res.Graph.Level = 0

		for id := uint64(0); id < uint64(n); id++ {
			conns := make([]uint64, linksPerNode)
			for j := range conns {
				conns[j] = uint64(r.Intn(n))
			}
			pc, err := packedconn.NewWithElements([][]uint64{conns})
			if err != nil {
				b.Fatal(err)
			}
			v := &ent.Vertex{ID: id, Level: 0, Connections: pc}
			// LinksReplaced bookkeeping mirrors what the real path does so
			// the merger sees Replace semantics.
			if res.Graph.LinksReplaced[id] == nil {
				res.Graph.LinksReplaced[id] = make(map[uint16]struct{})
			}
			res.Graph.LinksReplaced[id][0] = struct{}{}
			res.Graph.Nodes[id] = v
		}

		f, err := os.Create(path)
		if err != nil {
			b.Fatal(err)
		}
		sw := NewSortedWriter(f, logger)
		if err := sw.WriteAll(res); err != nil {
			b.Fatal(err)
		}
		f.Close()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		probe := startMemProbe(10 * time.Millisecond)

		iters := make([]IteratorLike, files)
		openFiles := make([]*os.File, files)
		for k, p := range paths {
			f, err := os.Open(p)
			if err != nil {
				b.Fatal(err)
			}
			openFiles[k] = f
			walReader := NewWALCommitReader(bufio.NewReaderSize(f, 256*1024), logger)
			it, err := NewIterator(walReader, k, logger)
			if err != nil {
				b.Fatal(err)
			}
			iters[k] = it
		}
		m, err := NewNWayMerger(iters, logger)
		if err != nil {
			b.Fatal(err)
		}
		nodesSeen := 0
		for {
			nc, err := m.Next()
			if err != nil {
				b.Fatal(err)
			}
			if nc == nil {
				break
			}
			nodesSeen++
		}
		for _, f := range openFiles {
			f.Close()
		}
		if nodesSeen == 0 {
			b.Fatal("no nodes merged")
		}

		probe.Stop()
		probe.report(b)
	}
}

func BenchmarkMemoryHighFanoutMerge_50k_2048_5files(b *testing.B) {
	benchmarkHighFanoutMerge(b, 50_000, 2_048, 5)
}

// =============================================================================
// Scenario S4: Raw WAL conversion.
//
// Drive a synthetic raw WAL through InMemoryReader.Do. The realistic upper
// bound is ~100MB per WAL file (defaultCommitLogSize / 5, commit_logger.go:74)
// before rotation. We don't try to hit 100MB on disk here — it would inflate
// the bench prologue — but we synthesize enough commits that the resulting
// DeserializationResult is comparable in shape to one produced from a full
// 100MB file (50k nodes, M=32, full level distribution).
// =============================================================================

func benchmarkRawWALConversion(b *testing.B, nodes int, commits int, m int) {
	b.Helper()

	var buf bytes.Buffer
	w := NewWALWriter(&buf)
	r := rand.New(rand.NewSource(7))
	for i := 0; i < commits; i++ {
		nodeID := uint64(r.Intn(nodes))
		level := uint16(0)
		// occasional level promotion to exercise multi-level paths
		for r.Float64() < 0.25 && level < 4 {
			level++
		}
		switch r.Intn(20) {
		case 0:
			if err := w.WriteAddNode(nodeID, level); err != nil {
				b.Fatal(err)
			}
		case 1:
			if err := w.WriteAddTombstone(nodeID); err != nil {
				b.Fatal(err)
			}
		default:
			// most commits are link writes
			conns := make([]uint64, r.Intn(m)+1)
			for j := range conns {
				conns[j] = uint64(r.Intn(nodes))
			}
			if err := w.WriteAddLinksAtLevel(nodeID, level, conns); err != nil {
				b.Fatal(err)
			}
		}
	}
	data := buf.Bytes()
	logger := testLogger()

	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		probe := startMemProbe(10 * time.Millisecond)

		walReader := NewWALCommitReader(bytes.NewReader(data), logger)
		memReader := NewInMemoryReader(walReader, logger)
		res, err := memReader.Do(nil, true)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Graph.Nodes) == 0 {
			b.Fatal("empty result")
		}

		probe.Stop()
		probe.report(b)
	}
}

func BenchmarkMemoryRawWALConversion_50k_2M(b *testing.B) {
	benchmarkRawWALConversion(b, 50_000, 2_000_000, 32)
}

// =============================================================================
// Scenario S10: Tombstone / LinksReplaced churn.
//
// Synthesize a WAL where every node sees AddTombstone -> RemoveTombstone
// cycles plus repeated ReplaceLinksAtLevel at multiple levels. The resulting
// DeserializationResult has Tombstones, TombstonesDeleted, and LinksReplaced
// maps that are *not* bounded by the live-set size — they grow with churn.
// =============================================================================

func benchmarkTombstoneChurn(b *testing.B, nodes int, churnPerNode int) {
	b.Helper()

	var buf bytes.Buffer
	w := NewWALWriter(&buf)
	for id := uint64(0); id < uint64(nodes); id++ {
		if err := w.WriteAddNode(id, 0); err != nil {
			b.Fatal(err)
		}
		for c := 0; c < churnPerNode; c++ {
			if err := w.WriteAddTombstone(id); err != nil {
				b.Fatal(err)
			}
			if err := w.WriteRemoveTombstone(id); err != nil {
				b.Fatal(err)
			}
			// also exercise LinksReplaced bookkeeping at multiple levels
			if err := w.WriteReplaceLinksAtLevel(id, uint16(c%6), []uint64{id ^ uint64(c)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	data := buf.Bytes()
	logger := testLogger()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		probe := startMemProbe(10 * time.Millisecond)

		walReader := NewWALCommitReader(bytes.NewReader(data), logger)
		memReader := NewInMemoryReader(walReader, logger)
		res, err := memReader.Do(nil, true)
		if err != nil {
			b.Fatal(err)
		}
		if res == nil {
			b.Fatal("nil result")
		}

		probe.Stop()
		probe.report(b)
	}
}

func BenchmarkMemoryTombstoneChurn_10k_x20(b *testing.B) {
	benchmarkTombstoneChurn(b, 10_000, 20)
}

// Suppress unused-import warning on logrus when nothing in this file uses it
// directly — testLogger() returns logrus.FieldLogger but we want the import
// to stay imported in case future scenarios need it.
var _ = logrus.New
