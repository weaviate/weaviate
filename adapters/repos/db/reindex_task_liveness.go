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

package db

import (
	"context"
	"sync"
	"time"
)

// reindexLivenessQueryTimeout bounds the leader round trip the first
// consulted lookup makes. Shard init runs on the RAFT apply goroutine
// for lazily loaded and multi-tenant shards, so a leader that is
// reachable but not answering would otherwise stall RAFT apply on this
// node for as long as it stays that way. A timeout answers "unknown",
// which is the non-destructive arm at every call site.
//
// 8s clears the cluster's own leader-discovery budget (≈5.55s at the
// default 1s election timeout, see backoffConfig in cluster/backoff.go).
// A shorter bound would expire inside leader discovery, so every lookup
// taken during an election would answer unknown and the residue would
// wait for the next shard load — and nothing re-runs this in between.
// The cost of the higher bound is paid only when the leader is
// unreachable, where it lengthens the per-apply stall described on
// [DB.reindexTaskLivenessLookup]; a reachable leader answers in
// milliseconds.
const reindexLivenessQueryTimeout = 8 * time.Second

// ReindexTaskLiveness is the three-way answer to "does the distributed
// task that wrote this on-disk migration state still own it?".
//
// The third value matters: startup code runs before the cluster state is
// guaranteed to be readable, and a decision that destroys data must
// never be taken on a guess.
type ReindexTaskLiveness int

const (
	// ReindexTaskLivenessUnknown means the distributed task list could
	// not be consulted. It is the zero value so a missing lookup can
	// never be mistaken for a definite answer.
	ReindexTaskLivenessUnknown ReindexTaskLiveness = iota
	// ReindexTaskLivenessLive means the task is still STARTED /
	// PREPARING / SWAPPING, so the reindex machinery owns its state.
	ReindexTaskLivenessLive
	// ReindexTaskLivenessDead means the task reached a terminal status
	// (FINISHED / CANCELLED / FAILED) or is no longer in the list at
	// all. Nothing will advance its on-disk state any further.
	ReindexTaskLivenessDead
)

func (l ReindexTaskLiveness) String() string {
	switch l {
	case ReindexTaskLivenessLive:
		return "live"
	case ReindexTaskLivenessDead:
		return "dead"
	default:
		return "unknown"
	}
}

// ReindexTaskLivenessLookup resolves the task identity persisted in a
// tracker dir's payload.mig (see [reindexRecoveryRecord]) against the
// distributed task list. A nil lookup answers "unknown".
type ReindexTaskLivenessLookup func(taskID string, taskVersion uint64) ReindexTaskLiveness

// Answer is the nil-safe way to call a lookup.
func (f ReindexTaskLivenessLookup) Answer(taskID string, taskVersion uint64) ReindexTaskLiveness {
	if f == nil || taskID == "" {
		return ReindexTaskLivenessUnknown
	}
	return f(taskID, taskVersion)
}

// reindexTaskLivenessLookup is the shard-side accessor. A shard whose
// index is not attached to a DB (unit tests, tooling) gets a lookup that
// answers "unknown".
func (s *Shard) reindexTaskLivenessLookup() ReindexTaskLivenessLookup {
	if s == nil || s.index == nil || s.index.db == nil {
		return nil
	}
	return s.index.db.reindexTaskLivenessLookup()
}

// reindexTaskLivenessLookup returns a lookup backed by the same
// distributed-task snapshot the orphan audit uses. Each returned lookup
// fetches at most one snapshot, and only if a caller actually asks —
// shard startup normally has no merged-but-untidied migration to decide
// about, and must not pay a cluster round trip for it.
//
// One lookup is minted per shard, so the snapshot is per-shard rather
// than per-process. That is deliberate: a shard activated hours after
// startup must not classify a task created since then as dead, which is
// the destructive arm. The cost of not sharing is one bounded query per
// shard that actually has such a migration to decide about: with an
// unreachable leader, one apply loading N such shards stalls for roughly
// ceil(N/(2·NCPU)) × [reindexLivenessQueryTimeout].
//
// The snapshot is unavailable while the audit deps are not installed
// yet (early startup) or when the task list cannot be read; both answer
// "unknown", which maps to the non-destructive arm at every call site.
func (db *DB) reindexTaskLivenessLookup() ReindexTaskLivenessLookup {
	var (
		once  sync.Once
		known KnownReindexTaskLookup
	)
	return func(taskID string, taskVersion uint64) ReindexTaskLiveness {
		once.Do(func() {
			if db == nil {
				return
			}
			db.reindexAuditMu.RLock()
			builder := db.reindexAuditLookupBuilder
			logger := db.reindexAuditLogger
			db.reindexAuditMu.RUnlock()
			if builder == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), reindexLivenessQueryTimeout)
			defer cancel()
			lookup, err := builder(ctx)
			if err != nil {
				if logger != nil {
					logger.Warnf("reindex task liveness: distributed task list unreadable, "+
						"treating every task as unknown: %v", err)
				}
				return
			}
			known = lookup
		})
		if known == nil {
			return ReindexTaskLivenessUnknown
		}
		if known(taskID, taskVersion) {
			return ReindexTaskLivenessLive
		}
		return ReindexTaskLivenessDead
	}
}
