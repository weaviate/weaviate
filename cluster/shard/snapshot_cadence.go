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

import "time"

// Snapshot cadence: when maybeSnapshot fires and at which index it snapshots.
//
// Three triggers, whichever fires first:
//
//   - entries: applied-index delta since the last snapshot (the original
//     trigger, SHARD_RAFT_SNAPSHOT_THRESHOLD);
//   - bytes: committed-entry byte volume retained above the last snapshot
//     (SHARD_RAFT_SNAPSHOT_BYTES_THRESHOLD). Bulk imports pack ~70–100
//     objects into one raft entry, so an entry-count threshold alone never
//     fires under exactly the load that needs compaction most: measured
//     3-node imports accumulated ~320MB of live payload in the shared bbolt
//     log and aged group-commit flushes from 15ms to >300ms;
//   - age: retained entries older than the jittered SnapshotMinInterval
//     (SHARD_RAFT_SNAPSHOT_MIN_INTERVAL, small-group floor). A small tenant
//     shard's handful of entries never reaches either threshold, so without
//     this floor its restart replay is unbounded in age — a measured
//     1000-tenant import replayed EVERY group's log from entry 1 on each
//     restart. Zero-cost for idle groups (no retained entries → two integer
//     compares); the snapshot index is still watermark-capped, so the age
//     trigger defers until the group's materialization is durably flushed.
//
// The snapshot index is the applied watermark, FLOORED at the minimum
// replication Match among peer voters while this node leads. The floor
// matters because groupStorage derives the raft-visible compaction horizon
// (FirstIndex / Term / Entries bounds) from the persisted snapshot metadata
// alone: the moment a snapshot at the applied index lands, the leader can no
// longer serve appends below it — a briefly-lagging follower (routine during
// import) would be demoted to a full snapshot + out-of-band state transfer
// even though the entries it needs are still physically present. Snapshotting
// AT the floor keeps the follower servable: Term(floor) is answered by the
// snapshot metadata and Entries(floor+1, …) by the retained log. A snapshot
// below the applied watermark is always valid here — restart seeds
// raft.Config.Applied from it and redelivers the suffix, which the FSM
// re-applies idempotently (the existing restart contract).
//
// The floor yields (escape) once the retained tail exceeds
// snapshotFloorEscapeMultiplier× either threshold, so one dead or wedged
// voter cannot pin the log — and the bbolt aging curve — forever; past the
// hatch the straggler legitimately falls back to snapshot + state transfer.

const (
	// defaultSnapshotBytesThreshold is the committed-byte delta that triggers
	// a snapshot when StoreConfig.SnapshotBytesThreshold is unset. 32MiB per
	// group keeps the measured 3-group import's shared-log live set (~3×
	// threshold high-water, bbolt files never shrink) well under the ~180MB
	// knee where group-commit flush latency starts aging, while a compaction
	// round deletes only ~100 bulk entries (~sub-ms bbolt tx) and the append
	// pipeline's snapshot barrier drains only every ~32MB of import volume.
	defaultSnapshotBytesThreshold = 32 * 1024 * 1024

	// snapshotFloorEscapeMultiplier bounds how far the Match floor may hold
	// compaction back: past N× the firing threshold the floor is ignored and
	// the snapshot lands at the applied watermark. Worst case with the 32MiB
	// default: a dead voter retains 128MiB of log for its group — degraded
	// flush latency but bounded and self-healing (the voter recovers via
	// state transfer, the tail compacts on the next trigger). Advancing-but-
	// slow followers stay floor-protected until they are genuinely N
	// thresholds behind.
	snapshotFloorEscapeMultiplier = 4

	// snapMarkGranularityDivisor sizes commitMarks checkpoint granularity as
	// bytesThreshold/divisor: coarse enough to bound the marks slice (≤
	// divisor marks per snapshot period, ×N under a pinned floor), fine
	// enough that the post-snapshot tail over-estimate (< one granule + one
	// entry) cannot re-fire the trigger on its own.
	snapMarkGranularityDivisor = 64
)

// Snapshot trigger names: the weaviate_shard_raft_snapshots_total "trigger"
// label and the cadence log line's trigger field.
const (
	snapshotTriggerEntries = "entries"
	snapshotTriggerBytes   = "bytes"
	snapshotTriggerAge     = "age"
)

// jitterMinInterval spreads the age-trigger deadline across groups: the
// returned interval is base scaled into [0.8, 1.2) by a stable per-group
// hash, so a node hosting thousands of small groups does not evaluate them
// all due in the same cadence window. Storms are additionally bounded by the
// snapshotter pool's Busy backpressure — the jitter exists to keep the
// steady state spread out, not as the only defense. Zero (disabled) and
// negative bases pass through unchanged.
func jitterMinInterval(base time.Duration, groupID uint64) time.Duration {
	if base <= 0 {
		return base
	}
	// splitmix64 finalizer: decorrelates sequential/patterned group IDs.
	h := groupID
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	frac := float64(h%1000) / 1000.0
	return time.Duration(float64(base) * (0.8 + 0.4*frac))
}

// markGranularity derives the commitMarks checkpoint granularity from the
// resolved byte threshold (floored at 1: every entry becomes a checkpoint).
func markGranularity(bytesThreshold uint64) uint64 {
	if g := bytesThreshold / snapMarkGranularityDivisor; g > 0 {
		return g
	}
	return 1
}

// byteMark is one (log index, cumulative committed bytes) checkpoint.
type byteMark struct {
	index uint64
	cum   uint64
}

// commitMarks measures the byte volume of committed entries retained above
// the last snapshot. Ready-loop-local: observed at commit staging
// (ackCommitted), rebased when a snapshot lands (pruneTo).
//
// The tail must survive a floored snapshot: when the snapshot index F lands
// below the applied watermark, the bytes in (F, applied] stay in the log and
// must keep counting toward the next trigger and the escape hatch — a plain
// reset-on-snapshot accumulator would forget a slow follower's growing lag
// and never trip the escape. The index→cum checkpoints make the rebase exact
// to one granule: cumAt(F) under-estimates by at most one granule plus one
// entry, over-estimating the tail — the conservative direction (compaction
// fires earlier, never later).
type commitMarks struct {
	granularity uint64
	marks       []byteMark // ascending index; all above the last pruneTo point
	cum         uint64     // total committed-entry bytes observed since Start
	base        uint64     // cum as of the last pruneTo (snapshot) index
}

// observe accounts one committed-staged entry and checkpoints the cumulative
// counter every granularity bytes.
func (m *commitMarks) observe(index, size uint64) {
	m.cum += size
	last := m.base
	if n := len(m.marks); n > 0 {
		last = m.marks[n-1].cum
	}
	if m.cum-last >= m.granularity {
		m.marks = append(m.marks, byteMark{index: index, cum: m.cum})
	}
}

// tail returns the committed bytes retained above the last snapshot index.
func (m *commitMarks) tail() uint64 { return m.cum - m.base }

// pruneTo rebases the accounting at a snapshot index: the tail keeps counting
// only bytes above it. Checkpoints at or below the index are dropped. A
// snapshot jumping past every checkpoint (a received install) rebases to the
// newest checkpoint, leaving at most one granule of residual tail.
func (m *commitMarks) pruneTo(index uint64) {
	i := 0
	for i < len(m.marks) && m.marks[i].index <= index {
		i++
	}
	if i == 0 {
		return
	}
	if c := m.marks[i-1].cum; c > m.base {
		m.base = c
	}
	m.marks = append(m.marks[:0], m.marks[i:]...)
}

// snapshotDue decides whether a snapshot fires, which threshold fired
// (trigger), and whether the retained backlog is so deep that the Match floor
// must be bypassed (escape). Pure, for table tests. A zero threshold disables
// the corresponding trigger (NewStore resolves both to non-zero defaults).
func snapshotDue(entriesDelta, tailBytes, entryThreshold, bytesThreshold uint64) (fire bool, trigger string, escape bool) {
	switch {
	case entryThreshold > 0 && entriesDelta >= entryThreshold:
		fire, trigger = true, snapshotTriggerEntries
	case bytesThreshold > 0 && tailBytes >= bytesThreshold:
		fire, trigger = true, snapshotTriggerBytes
	default:
		return false, "", false
	}
	escape = (entryThreshold > 0 && entriesDelta >= snapshotFloorEscapeMultiplier*entryThreshold) ||
		(bytesThreshold > 0 && tailBytes >= snapshotFloorEscapeMultiplier*bytesThreshold)
	return fire, trigger, escape
}

// snapshotTarget picks the snapshot index: the applied watermark, floored at
// the minimum peer-voter Match (haveFloor false on followers and single-voter
// groups — they serve no one, so they compact on their own applied progress).
// Pure, for table tests.
func snapshotTarget(applied, matchFloor uint64, haveFloor, escape bool) (target uint64, floorCapped bool) {
	if escape || !haveFloor || matchFloor >= applied {
		return applied, false
	}
	return matchFloor, true
}

// snapshotIndex composes the final snapshot index: the Match-floored cadence
// target (snapshotTarget) capped at durableFloor — the highest applied index
// whose materialization is durable in flushed LSM segments (the shard's
// DurableRaftFloor). The cap is a hard durability bound and is deliberately
// NOT subject to the escape hatch: the escape trades a laggard's servability
// for compaction, but compacting past the durable floor would discard
// entries whose only materialization is in un-flushed memtables — data loss
// on crash, not a service trade-off. The cap derives from the flushed
// applied index, never from the floored target, so a Match-floored snapshot
// below the flush watermark is unaffected by it. Pure, for table tests.
func snapshotIndex(applied, matchFloor uint64, haveFloor, escape bool,
	durableFloor uint64,
) (target uint64, floorCapped, wmCapped bool) {
	target, floorCapped = snapshotTarget(applied, matchFloor, haveFloor, escape)
	if durableFloor < target {
		return durableFloor, floorCapped, true
	}
	return target, floorCapped, false
}

// pendingSnapMeta carries the in-flight snapshot's trigger context from
// submit (maybeSnapshot) to persist completion (completeLocalSnapshot) for
// the cadence log line and counter. Ready-loop-local; meaningful only while
// snapshotPending.
type pendingSnapMeta struct {
	trigger     string
	tailBytes   uint64
	floorCapped bool
	wmCapped    bool
	escape      bool
}
