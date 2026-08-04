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

import "sync"

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
// distributed-task snapshot the orphan audit uses. The snapshot is
// fetched at most once, and only if a caller actually asks — shard
// startup normally has no merged-but-untidied migration to decide
// about, and must not pay a cluster round trip for it.
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
			lookup, err := builder()
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
