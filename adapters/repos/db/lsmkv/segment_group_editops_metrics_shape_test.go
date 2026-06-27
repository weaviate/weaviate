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

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

const editOpsDurationFam = "weaviate_lsm_segment_edit_ops_transformer_duration_seconds"

// histogramCount returns the observation count of the series in family `name`
// labelled `want`, and whether that series exists.
func histogramCount(t *testing.T, g prometheus.Gatherer, name string, want map[string]string) (uint64, bool) {
	t.Helper()
	families, err := g.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if len(labels) != len(want) {
				continue
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
				}
			}
			if match {
				return m.GetHistogram().GetSampleCount(), true
			}
		}
	}
	return 0, false
}

// TestEditOpsMetrics_HistogramRecordsEveryAttempt pins the histogram's only
// production observation site, and pins that it counts FAILED attempts too: a
// segment quarantined after five failed rewrites spent that time five times
// over, and a histogram observing only successes describes the easy cases
// while omitting the pathological ones it is most wanted for.
func TestEditOpsMetrics_HistogramRecordsEveryAttempt(t *testing.T) {
	opType := map[string]string{"op_type": string(OpTypeRemoveTargetVectors)}

	t.Run("a successful rewrite is observed", func(t *testing.T) {
		bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
		require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
		require.NoError(t, bucket.FlushAndSwitch())
		require.NoError(t, editOps.RegisterOp("op1",
			OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
		require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))
		installEditOpsMetrics(t, bucket, "EditOpsHistogramOkClass")

		before, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
		cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
		require.NoError(t, err)
		require.True(t, cleaned)

		after, ok := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
		require.True(t, ok, "the cleanup pass must observe the transformer duration")
		require.Equal(t, before+1, after)
	})

	t.Run("a failed rewrite is observed too", func(t *testing.T) {
		bucket, editOps := newReplaceBucketWithEditOps(t, failingTransformer)
		require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
		require.NoError(t, bucket.FlushAndSwitch())
		require.NoError(t, editOps.RegisterOp("op1",
			OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
		require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))
		installEditOpsMetrics(t, bucket, "EditOpsHistogramFailClass")

		before, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
		// A failed rewrite is deliberately NOT surfaced as a pass error — the
		// retry budget decides what happens next, not the cycle — so the
		// precondition is that the segment is still owed and has accrued an
		// attempt, not that cleanupOnce returned an error.
		cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
		require.NoError(t, err)
		require.False(t, cleaned, "precondition: the rewrite must not have succeeded")
		pending, err := editOps.AllPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, 1, pending[0].Attempts, "precondition: the failure was counted")

		after, ok := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
		require.True(t, ok)
		require.Equal(t, before+1, after,
			"the time a failed attempt spent in the transformer was still spent")
	})

	t.Run("a compaction is not observed", func(t *testing.T) {
		// The carve-out the Help text promises: a compaction runs the same
		// transformer, but findCompactionCandidates picked that merge for its
		// own reasons, so billing it to the edit op would report work the drop
		// did not cause.
		bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
		require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
		require.NoError(t, bucket.FlushAndSwitch())
		require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
		require.NoError(t, bucket.FlushAndSwitch())
		require.NoError(t, editOps.RegisterOp("op1",
			OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
		require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))
		installEditOpsMetrics(t, bucket, "EditOpsHistogramCompactionClass")

		before, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
		compacted, err := bucket.disk.compactOnce(context.Background())
		require.NoError(t, err)
		require.True(t, compacted, "precondition: the merge must have happened")

		after, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
		require.Equal(t, before, after,
			"compaction must not be billed to the edit op")
	})
}

// TestEditOpsMetrics_HistogramSkipsAbortedRewrites pins the abort carve-out at
// the observation site. shouldAbort cuts the transform off mid-segment (sampled
// every 100 keys), so the elapsed time is a truncated fraction of the real cost
// — and the retry budget refuses to count the same event as an attempt. The two
// readings of one event must agree: neither an observation nor an attempt.
func TestEditOpsMetrics_HistogramSkipsAbortedRewrites(t *testing.T) {
	opType := map[string]string{"op_type": string(OpTypeRemoveTargetVectors)}

	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	// Enough keys to reach the abort sample point (every 100th key).
	for i := range 150 {
		require.NoError(t, bucket.Put(fmt.Appendf(nil, "k%03d", i), []byte("v")))
	}
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))
	installEditOpsMetrics(t, bucket, "EditOpsHistogramAbortClass")

	before, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
	// False on the first call so the pass survives its pre-rewrite gate and
	// actually enters the transformer; true from the in-loop sample (every
	// 100th key) on, so the abort lands MID-transform — an always-true
	// callback would bail before the rewrite and prove nothing.
	calls := 0
	abortMidTransform := func() bool { calls++; return calls > 1 }
	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(abortMidTransform)
	require.NoError(t, err)
	require.False(t, cleaned, "precondition: the pass must have aborted")
	require.Greater(t, calls, 1, "precondition: the in-loop sample must have fired")

	pending, err := editOps.AllPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Zero(t, pending[0].Attempts, "an abort is not a failed attempt")

	after, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
	require.Equal(t, before, after,
		"a truncated transform is not a sample; the retry budget already refuses to count it")
}

// TestEditOpsMetrics_HistogramDedupsOpTypes pins distinctOpTypes' dedup. One
// pass applies every active op's transformer at once and attributes the cost
// per op TYPE present — two concurrent drops (same type) must produce one
// observation per pass, not one per op, or the histogram double-counts exactly
// when the machinery is busiest.
func TestEditOpsMetrics_HistogramDedupsOpTypes(t *testing.T) {
	opType := map[string]string{"op_type": string(OpTypeRemoveTargetVectors)}

	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.NoError(t, editOps.RegisterOp("opA",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("opA", segs))
	require.NoError(t, editOps.RegisterOp("opB",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 2}))
	require.NoError(t, editOps.SnapshotSegments("opB", segs))
	installEditOpsMetrics(t, bucket, "EditOpsHistogramDedupClass")

	before, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	after, _ := histogramCount(t, prometheus.DefaultGatherer, editOpsDurationFam, opType)
	require.Equal(t, before+1, after,
		"two ops of one type share the pass, so its cost is one sample, not two")
}

// TestEditOpsMetrics_AttributesPerOp pins that the gauges are computed PER OP
// rather than from a package total. Every other metrics test arms a single op
// called op1, under which "the count for this op" and "the count for all ops"
// are the same number — so a refresh that published the total everywhere stayed
// green. Two concurrent drops on one shard are reachable in production: drop
// vecB while vecA is still stripping.
func TestEditOpsMetrics_AttributesPerOp(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	for _, k := range []string{"k1", "k2", "k3"} {
		require.NoError(t, bucket.Put([]byte(k), []byte("v")))
		require.NoError(t, bucket.FlushAndSwitch())
	}
	segs := segIDsOf(bucket)
	require.Len(t, segs, 3)

	const className = "EditOpsPerOpClass"
	installEditOpsMetrics(t, bucket, className)

	// Two ops of the SAME type, owing DIFFERENT numbers of segments.
	require.NoError(t, editOps.RegisterOp("opA",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("opA", segs))
	require.NoError(t, editOps.RegisterOp("opB",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 2}))
	require.NoError(t, editOps.SnapshotSegments("opB", segs[:1]))

	require.NoError(t, bucket.disk.recoverEditOps(t.Context()))

	a, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "opA"))
	require.True(t, ok)
	require.Equal(t, float64(3), a, "opA owes three segments, not the shard total")

	b, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "opB"))
	require.True(t, ok)
	require.Equal(t, float64(1), b, "opB owes one, and must not inherit opA's count")

	// Same for owed, via a quarantine that only opA suffers.
	require.NoError(t, editOps.Quarantine("opA", segs[2]))
	require.NoError(t, bucket.disk.recoverEditOps(t.Context()))

	owedA, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "opA"))
	require.True(t, ok)
	require.Equal(t, float64(3), owedA, "quarantined is still owed")
	pendA, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "opA"))
	require.True(t, ok)
	require.Equal(t, float64(2), pendA, "but it has left the queue")

	owedB, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "opB"))
	require.True(t, ok)
	require.Equal(t, float64(1), owedB, "opB must not show opA's stall")

	// The active gauge counts OPS per type, so two ops of one type read 2.
	active, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok)
	require.Equal(t, float64(2), active, "two ops of the same type must count as two")
}

// TestEditOpsMetrics_LockedSidecarWaitsForOneFlockTimeoutOnly pins the
// ErrTimeout guard on recoverEditOps' deferred refresh. When a previous
// instance still holds the sidecar's flock, Recover fails after one
// BoltFlockTimeout and the load is failed so the shard lifecycle retries; an
// unguarded refresh re-opens bolt and eats a SECOND full timeout on exactly
// the path that is already failing, doubling shard-load failure latency.
// Inherently a ~5s test: the timeout is a const and the wait is the subject.
func TestEditOpsMetrics_LockedSidecarWaitsForOneFlockTimeoutOnly(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))
	installEditOpsMetrics(t, bucket, "EditOpsLockedSidecarClass")

	// Model the fresh instance meeting a still-held flock: release our handle
	// (Close leaves the closed *bolt.DB in place, so nil it — a fresh load
	// starts with no handle and must re-open), then take the flock from a
	// second open, standing in for the previous instance that has not finished
	// closing.
	require.NoError(t, editOps.Close())
	editOps.mu.Lock()
	editOps.db = nil
	editOps.mu.Unlock()
	locker, err := bolt.Open(filepath.Join(bucket.disk.dir, segmentEditOpsFileName), 0o600, nil)
	require.NoError(t, err)
	defer locker.Close()

	start := time.Now()
	err = bucket.disk.recoverEditOps(context.Background())
	elapsed := time.Since(start)
	require.ErrorIs(t, err, bolterrors.ErrTimeout, "precondition: the flock must be contended")
	require.Less(t, elapsed, entlsmkv.BoltFlockTimeout+3*time.Second,
		"the failing path must wait out ONE flock timeout, not a second one in the refresh")
}

// TestRefreshEditOpsMetrics_KeepsGaugesWhenTheSidecarReadFails pins the error
// return in the refresh. Swallowing a read error is not a no-op: the three
// slices come back nil, so every seen op type is zeroed to active=0 and every
// seen op id is forgotten — a transient blip would publish a finished-drop
// reading over a live strip, and nothing would restore it until the next
// lifecycle event.
func TestRefreshEditOpsMetrics_KeepsGaugesWhenTheSidecarReadFails(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	const className = "EditOpsSnapshotErrorClass"
	installEditOpsMetrics(t, bucket, className)
	bucket.disk.refreshEditOpsMetrics()

	active, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "precondition: the live strip is published")
	require.Equal(t, float64(1), active)
	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok)
	require.Equal(t, float64(1), owed)

	// Break the sidecar underneath: close the handle and truncate the file, so
	// the next read fails instead of returning an empty-but-valid database.
	require.NoError(t, editOps.Close())
	editOps.mu.Lock()
	editOps.db = nil
	editOps.mu.Unlock()
	require.NoError(t, os.WriteFile(
		filepath.Join(bucket.disk.dir, segmentEditOpsFileName), []byte("not a bolt file"), 0o600))

	bucket.disk.refreshEditOpsMetrics()

	active, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "a failed read must not retire the active series")
	require.Equal(t, float64(1), active, "a failed read must not publish a finished-drop reading")
	owed, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok, "a failed read must not forget the op's series")
	require.Equal(t, float64(1), owed)
}

// TestMetricsSnapshot_IsOneConsistentRead pins the single-transaction read.
// Reading ops, pending and quarantined in three separate transactions lets a
// Quarantine commit land between two of them, so the same segment is seen in
// both buckets and counted twice in owed — inflating exactly the gauge read as
// the stall signal, in the direction that fabricates a stall.
//
// Asserted by counting bolt transactions rather than by racing a writer against
// a reader. The torn window is real but tiny — a read tx takes microseconds and
// a write tx fsyncs — so a concurrency test hits it too rarely to be a
// regression guard, and would pass on a three-transaction implementation almost
// every run. The transaction count is the same property, deterministically.
func TestMetricsSnapshot_IsOneConsistentRead(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	// The bolt file opens lazily, so make sure it is open before counting.
	_, _, _, err := editOps.MetricsSnapshot()
	require.NoError(t, err)
	require.NotNil(t, editOps.db, "precondition: the sidecar must be open")

	before := editOps.db.Stats().TxN
	ops, pending, quarantined, err := editOps.MetricsSnapshot()
	require.NoError(t, err)
	require.Equal(t, before+1, editOps.db.Stats().TxN,
		"all three sets must come from ONE transaction, or owed can double-count a "+
			"segment a concurrent Quarantine moved between two reads")

	// And it must actually return the state, not an empty consistent view.
	require.Len(t, ops, 1)
	require.Len(t, pending, 1)
	require.Empty(t, quarantined)
}

// TestRefreshEditOpsMetrics_IsSerialised pins editOpsMetricsLock — but ONLY
// under -race, which is how CI runs this package. The refresh reads and then
// rewrites the seen-op and seen-type sets, so two racing interleave and the one
// holding the older view can re-publish an op the other just forgot. The final
// assertion is a sanity floor, not a race detector: without -race the lockless
// mutation passes it, because the interleaving rarely corrupts this one value.
func TestRefreshEditOpsMetrics_IsSerialised(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	const className = "EditOpsSerialisedClass"
	installEditOpsMetrics(t, bucket, className)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				bucket.disk.refreshEditOpsMetrics()
			}
		}()
	}
	wg.Wait()

	pending, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.True(t, ok, "concurrent refreshes must not lose the series")
	require.Equal(t, float64(1), pending)
}
