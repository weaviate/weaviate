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

package sharedlog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The shared log is the one write path every shard-raft group on a node funnels
// through; its flush latency is the coupling coefficient between groups. The
// two histograms are the primary evidence for (or against) the shared-log
// coupling hypothesis: flush_seconds rising with batch_size flat means the disk
// is slow; batch_size pinned at the max means groups are queueing on each other.
// The segments gauge makes the reclamation trade observable: the rewrite policy
// deliberately lets several mostly-dead segments accumulate before copying an
// idle group's residue forward (see the policy constants in wal.go).
var (
	flushSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_sharedlog_flush_seconds",
		Help:    "Duration of one shared raft log batch commit (one WAL fsync).",
		Buckets: prometheus.ExponentialBuckets(0.0005, 4, 10),
	})
	flushBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_sharedlog_flush_batch_size",
		Help:    "Group writes coalesced into one shared raft log batch commit.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
	})
	segmentsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "weaviate_shard_raft_sharedlog_segments",
		Help: "WAL segment files currently on disk for the shared raft log.",
	})
	// splitBrainReads counts reads that hit the index split-brain guard: an
	// entry position inside the raft-visible range with no retained entry and
	// no covering snapshot (minor-issues.md #9). Zero on a healthy node; any
	// increase means a group's compaction outlived its authorizing snapshot
	// and its stragglers are being parked by ErrSnapshotTemporarilyUnavailable.
	splitBrainReads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_sharedlog_splitbrain_reads_total",
		Help: "Reads answered by the WAL index split-brain guard (visible range not backed by entries or a snapshot).",
	})
	// poisonedGroupsGauge counts groups quarantined by WAL boot validation;
	// their stores refuse to start until the group is dropped.
	poisonedGroupsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "weaviate_shard_raft_sharedlog_poisoned_groups",
		Help: "Raft groups quarantined at WAL boot validation (store start refused).",
	})
)
