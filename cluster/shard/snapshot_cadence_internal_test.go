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

package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMarkGranularity(t *testing.T) {
	tests := []struct {
		name           string
		bytesThreshold uint64
		want           uint64
	}{
		{name: "default threshold", bytesThreshold: 32 * 1024 * 1024, want: 512 * 1024},
		{name: "small test threshold", bytesThreshold: 64 * 1024, want: 1024},
		{name: "below divisor floors at one", bytesThreshold: snapMarkGranularityDivisor - 1, want: 1},
		{name: "zero floors at one", bytesThreshold: 0, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, markGranularity(tc.bytesThreshold))
		})
	}
}

// TestSnapshotDue pins the trigger policy: entries first (the original
// trigger keeps precedence when both fire), bytes second, and the escape
// evaluated against BOTH thresholds independently of which one fired.
func TestSnapshotDue(t *testing.T) {
	const (
		entryThresh = uint64(100)
		bytesThresh = uint64(1000)
	)
	tests := []struct {
		name         string
		entriesDelta uint64
		tailBytes    uint64
		entryThresh  uint64
		bytesThresh  uint64
		wantFire     bool
		wantTrigger  string
		wantEscape   bool
	}{
		{name: "neither threshold reached", entriesDelta: 99, tailBytes: 999, entryThresh: entryThresh, bytesThresh: bytesThresh},
		{name: "entries at threshold", entriesDelta: 100, tailBytes: 0, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerEntries},
		{name: "bytes at threshold", entriesDelta: 5, tailBytes: 1000, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerBytes},
		{name: "both fire: entries wins", entriesDelta: 100, tailBytes: 1000, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerEntries},
		{name: "entries deep past escape", entriesDelta: 400, tailBytes: 0, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerEntries, wantEscape: true},
		{name: "bytes deep past escape", entriesDelta: 5, tailBytes: 4000, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerBytes, wantEscape: true},
		{name: "entries fired but bytes tail trips escape", entriesDelta: 100, tailBytes: 4000, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerEntries, wantEscape: true},
		{name: "one short of escape", entriesDelta: 399, tailBytes: 3999, entryThresh: entryThresh, bytesThresh: bytesThresh, wantFire: true, wantTrigger: snapshotTriggerEntries},
		{name: "zero entry threshold disables entries", entriesDelta: 1 << 30, tailBytes: 999, entryThresh: 0, bytesThresh: bytesThresh},
		{name: "zero bytes threshold disables bytes", entriesDelta: 99, tailBytes: 1 << 40, entryThresh: entryThresh, bytesThresh: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fire, trigger, escape := snapshotDue(tc.entriesDelta, tc.tailBytes, tc.entryThresh, tc.bytesThresh)
			require.Equal(t, tc.wantFire, fire)
			require.Equal(t, tc.wantTrigger, trigger)
			require.Equal(t, tc.wantEscape, escape)
		})
	}
}

func TestSnapshotTarget(t *testing.T) {
	tests := []struct {
		name       string
		applied    uint64
		matchFloor uint64
		haveFloor  bool
		escape     bool
		wantTarget uint64
		wantCapped bool
	}{
		{name: "no floor (follower / single voter)", applied: 50, wantTarget: 50},
		{name: "floor below applied caps", applied: 50, matchFloor: 20, haveFloor: true, wantTarget: 20, wantCapped: true},
		{name: "floor at applied", applied: 50, matchFloor: 50, haveFloor: true, wantTarget: 50},
		{name: "floor ahead of applied (apply lag)", applied: 50, matchFloor: 80, haveFloor: true, wantTarget: 50},
		{name: "escape overrides floor", applied: 50, matchFloor: 20, haveFloor: true, escape: true, wantTarget: 50},
		{name: "never-contacted voter pins at zero", applied: 50, matchFloor: 0, haveFloor: true, wantTarget: 0, wantCapped: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target, capped := snapshotTarget(tc.applied, tc.matchFloor, tc.haveFloor, tc.escape)
			require.Equal(t, tc.wantTarget, target)
			require.Equal(t, tc.wantCapped, capped)
		})
	}
}

// TestSnapshotIndex pins the durable-floor cap composition: the cap only
// ever LOWERS the Match-floored target (never raises it), it derives from
// the flushed applied index rather than the floored target, and — unlike the
// Match floor — it is never bypassed by the escape hatch: the escape trades
// a laggard's servability for compaction, while the durable floor guards
// against discarding entries whose materialization is not yet flushed.
func TestSnapshotIndex(t *testing.T) {
	const max = ^uint64(0)
	tests := []struct {
		name            string
		applied         uint64
		matchFloor      uint64
		haveFloor       bool
		escape          bool
		durableFloor    uint64
		wantTarget      uint64
		wantFloorCapped bool
		wantWmCapped    bool
	}{
		{
			name: "clean shard imposes no cap", applied: 50, durableFloor: max,
			wantTarget: 50,
		},
		{
			name: "watermark below applied caps", applied: 50, durableFloor: 30,
			wantTarget: 30, wantWmCapped: true,
		},
		{
			name: "watermark at target is not a cap", applied: 50, durableFloor: 50,
			wantTarget: 50,
		},
		{
			name: "watermark above applied never raises the index", applied: 50, durableFloor: 80,
			wantTarget: 50,
		},
		{
			name: "watermark caps below the Match floor", applied: 50, matchFloor: 40, haveFloor: true, durableFloor: 20,
			wantTarget: 20, wantFloorCapped: true, wantWmCapped: true,
		},
		{
			name: "Match floor binds tighter than the watermark", applied: 50, matchFloor: 20, haveFloor: true, durableFloor: 40,
			wantTarget: 20, wantFloorCapped: true,
		},
		{
			name: "escape bypasses the Match floor but never the watermark", applied: 50, matchFloor: 20, haveFloor: true, escape: true, durableFloor: 30,
			wantTarget: 30, wantWmCapped: true,
		},
		{
			name: "never-flushed dirty shard pins at zero", applied: 50, escape: true, durableFloor: 0,
			wantTarget: 0, wantWmCapped: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target, floorCapped, wmCapped := snapshotIndex(
				tc.applied, tc.matchFloor, tc.haveFloor, tc.escape, tc.durableFloor)
			require.Equal(t, tc.wantTarget, target)
			require.Equal(t, tc.wantFloorCapped, floorCapped)
			require.Equal(t, tc.wantWmCapped, wmCapped)
		})
	}
}

// TestJitterMinInterval pins the age-floor jitter contract: disabled passes
// through, jittered deadlines stay within ±20% of base, the value is stable
// per group, and distinct groups genuinely spread out (the anti-storm
// property for nodes hosting thousands of small groups).
func TestJitterMinInterval(t *testing.T) {
	require.Zero(t, jitterMinInterval(0, 42), "disabled (0) must pass through")

	base := 10 * time.Minute
	seen := map[time.Duration]struct{}{}
	for g := uint64(0); g < 64; g++ {
		j := jitterMinInterval(base, g)
		require.GreaterOrEqual(t, j, 8*time.Minute, "group %d below -20%% bound", g)
		require.Less(t, j, 12*time.Minute, "group %d above +20%% bound", g)
		require.Equal(t, j, jitterMinInterval(base, g), "group %d must get a stable deadline", g)
		seen[j] = struct{}{}
	}
	require.Greater(t, len(seen), 32, "sequential group IDs must spread, not cluster")
}

// TestCommitMarks drives observe/tail/pruneTo sequences: checkpoint
// coalescing at the granularity, conservative rebasing (cumAt
// under-estimates → tail over-estimates, never the reverse), tail survival
// across a floored snapshot, and a received-install jump past every mark.
func TestCommitMarks(t *testing.T) {
	type op struct {
		observeIdx  uint64 // observe(observeIdx, observeSize) when observeSize > 0
		observeSize uint64
		pruneTo     uint64 // pruneTo(pruneTo) when > 0
		wantTail    uint64
		wantMarks   int
	}
	tests := []struct {
		name        string
		granularity uint64
		ops         []op
	}{
		{
			name:        "granularity one marks every entry",
			granularity: 1,
			ops: []op{
				{observeIdx: 1, observeSize: 5, wantTail: 5, wantMarks: 1},
				{observeIdx: 2, observeSize: 5, wantTail: 10, wantMarks: 2},
				{pruneTo: 1, wantTail: 5, wantMarks: 1},
				{pruneTo: 2, wantTail: 0, wantMarks: 0},
			},
		},
		{
			name:        "sub-granularity entries coalesce into one mark",
			granularity: 10,
			ops: []op{
				{observeIdx: 1, observeSize: 4, wantTail: 4, wantMarks: 0},
				{observeIdx: 2, observeSize: 4, wantTail: 8, wantMarks: 0},
				{observeIdx: 3, observeSize: 4, wantTail: 12, wantMarks: 1}, // crossed 10
				{observeIdx: 4, observeSize: 4, wantTail: 16, wantMarks: 1},
			},
		},
		{
			name:        "prune between marks rebases conservatively",
			granularity: 10,
			ops: []op{
				{observeIdx: 1, observeSize: 10, wantTail: 10, wantMarks: 1},
				{observeIdx: 2, observeSize: 4, wantTail: 14, wantMarks: 1},
				{observeIdx: 3, observeSize: 10, wantTail: 24, wantMarks: 2},
				// Snapshot at 2: only the mark at 1 is ≤ 2, so the 4 bytes at
				// index 2 stay in the tail — over-estimate, never under.
				{pruneTo: 2, wantTail: 14, wantMarks: 1},
			},
		},
		{
			name:        "prune below every mark is a no-op",
			granularity: 10,
			ops: []op{
				{observeIdx: 5, observeSize: 4, wantTail: 4, wantMarks: 0},
				{observeIdx: 6, observeSize: 4, wantTail: 8, wantMarks: 0},
				{pruneTo: 3, wantTail: 8, wantMarks: 0},
			},
		},
		{
			name:        "floored snapshot keeps the follower-lag tail counting",
			granularity: 1,
			ops: []op{
				{observeIdx: 10, observeSize: 100, wantTail: 100, wantMarks: 1},
				{observeIdx: 20, observeSize: 100, wantTail: 200, wantMarks: 2},
				{observeIdx: 30, observeSize: 100, wantTail: 300, wantMarks: 3},
				// Snapshot floored at Match=10: the 200 bytes above it keep
				// counting toward the next trigger and the escape hatch.
				{pruneTo: 10, wantTail: 200, wantMarks: 2},
				{observeIdx: 40, observeSize: 100, wantTail: 300, wantMarks: 3},
			},
		},
		{
			name:        "received install jumps past every mark",
			granularity: 10,
			ops: []op{
				{observeIdx: 1, observeSize: 10, wantTail: 10, wantMarks: 1},
				{observeIdx: 2, observeSize: 10, wantTail: 20, wantMarks: 2},
				{observeIdx: 3, observeSize: 4, wantTail: 24, wantMarks: 2},
				// Install at index 1000, far above all local marks: rebase to
				// the newest mark — only the sub-granularity residual remains.
				{pruneTo: 1000, wantTail: 4, wantMarks: 0},
				{observeIdx: 1001, observeSize: 6, wantTail: 10, wantMarks: 1}, // residual + 6 crosses 10
			},
		},
		{
			name:        "backward prune never regresses the base",
			granularity: 1,
			ops: []op{
				{observeIdx: 1, observeSize: 5, wantTail: 5, wantMarks: 1},
				{observeIdx: 2, observeSize: 5, wantTail: 10, wantMarks: 2},
				{pruneTo: 2, wantTail: 0, wantMarks: 0},
				{pruneTo: 1, wantTail: 0, wantMarks: 0},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := commitMarks{granularity: tc.granularity}
			for i, o := range tc.ops {
				if o.observeSize > 0 {
					m.observe(o.observeIdx, o.observeSize)
				}
				if o.pruneTo > 0 {
					m.pruneTo(o.pruneTo)
				}
				require.Equalf(t, o.wantTail, m.tail(), "op %d: tail", i)
				require.Equalf(t, o.wantMarks, len(m.marks), "op %d: marks", i)
			}
		})
	}
}
