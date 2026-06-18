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

//go:build integrationTest

package compact

// Regression gate for the SnapshotWriter sparse-ID peak.
//
// Before the sparse-storage fix, SnapshotWriter.nodes was a slot-indexed
// []*nodeState slice grown via ensureNodesCapacity, so peak heap during
// snapshot creation scaled with maxNodeID rather than with the live-set
// size. For 100 k live nodes scattered across a 1×10⁸ ID space peak
// exceeded 1.8 GB; with sparse storage it sits around 30 MB on the same
// input.
//
// This test pins that ceiling. If it ever fails again, suspect a re-
// introduction of slot-indexed storage in the snapshot path (writer,
// reader, or a new caller materializing the full slot space).
//
//   go test -tags=integrationTest -count=1 -run=TestSparseSnapshotPeakRegression \
//     ./adapters/repos/db/vector/hnsw/compact/

import (
	"io"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// SparseSnapshotPeakLimit is the hard ceiling for the sparse-ID scenario.
// Current measurement is ~30 MB peak; 100 MB leaves headroom for sampling
// noise + payload growth, while still failing by 10× if slot-indexed
// storage is reintroduced.
const SparseSnapshotPeakLimitMB = 100

const (
	regressionLiveNodes = 100_000
	regressionIDSpread  = uint64(100_000_000)
)

func TestSparseSnapshotPeakRegression(t *testing.T) {
	r := rand.New(rand.NewSource(int64(regressionLiveNodes) ^ int64(regressionIDSpread)))
	ids := make([]uint64, regressionLiveNodes)
	for i := range ids {
		ids[i] = uint64(r.Int63n(int64(regressionIDSpread)))
	}
	// AddNode tolerates out-of-order, but the realistic NWayMerger feed is
	// ascending. Sort to match.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}

	runtime.GC()
	probe := startMemProbe(10 * time.Millisecond)
	t.Cleanup(probe.Stop)

	w := NewSnapshotWriter(io.Discard)
	w.SetEntrypoint(ids[0], 1)
	for _, id := range ids {
		w.AddNode(id, 0, [][]uint64{{}}, false)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	probe.Stop()
	peakMB := probe.peak.Load() / (1024 * 1024)
	t.Logf("sparse-ID snapshot peak: %d MB (limit %d MB; live nodes %d, ID spread %d)",
		peakMB, SparseSnapshotPeakLimitMB, regressionLiveNodes, regressionIDSpread)

	if peakMB > SparseSnapshotPeakLimitMB {
		t.Fatalf(
			"SnapshotWriter peak heap %d MB exceeds %d MB regression limit. "+
				"This usually means slot-indexed storage was reintroduced "+
				"somewhere on the snapshot path. Cross-check the bench "+
				"BenchmarkMemorySparseSnapshot for trend.",
			peakMB, SparseSnapshotPeakLimitMB,
		)
	}
}
