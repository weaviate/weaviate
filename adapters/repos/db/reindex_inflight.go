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
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// unwiredGateWarnOnce ensures the operator-facing WARN for the
// "lookup-not-installed" path fires at most once per process. The
// warning is informational: production gates HTTP serving on bootstrap
// completion, so under normal startup the unwired window is unreachable
// by an external backup request. If the WARN does fire in production
// logs, it means either (a) startup ordering is broken (lookup wiring
// never fires) or (b) a non-HTTP code path called Backupable before
// the lookup installed.
var unwiredGateWarnOnce sync.Once

// reindexGate resolves the backup-gate lookups once — the activity one
// is a cluster-wide RAFT query — and judges every shard a caller checks
// against that one snapshot. Resolution is lazy: unused, it never queries.
type reindexGate struct {
	db       *DB
	once     sync.Once
	activity ShardReindexActivityLookup
	cleanup  CleanupInProgressLookup
}

func newReindexGate(db *DB) *reindexGate {
	return &reindexGate{db: db}
}

// reindexGateCtxKey carries a pass-scoped gate to shards. The shard call
// sites sit behind [ShardLike], so a parameter would cascade through the
// interface and its generated mocks.
type reindexGateCtxKey struct{}

func contextWithReindexGate(ctx context.Context, gate *reindexGate) context.Context {
	return context.WithValue(ctx, reindexGateCtxKey{}, gate)
}

// reindexGateFromContext returns the gate of the backup pass ctx belongs
// to, or nil. Replica movement and offload are not passes: they install
// no gate and keep resolving a fresh snapshot per call.
func reindexGateFromContext(ctx context.Context) *reindexGate {
	gate, _ := ctx.Value(reindexGateCtxKey{}).(*reindexGate)
	return gate
}

func (g *reindexGate) resolve() {
	g.once.Do(func() {
		// Read at resolve time: a gate built between the two install
		// calls must still see both builders, not just whichever landed first.
		g.db.reindexAuditMu.RLock()
		activityBuilder := g.db.shardReindexActivityLookupBuilder
		cleanupBuilder := g.db.reindexCleanupInProgressLookupBldr
		g.db.reindexAuditMu.RUnlock()

		if activityBuilder == nil {
			unwiredGateWarnOnce.Do(func() {
				logger := g.db.logger
				if logger == nil {
					logger = logrus.New()
				}
				logger.WithField("action", "backup_reindex_gate").
					Warn("backup-reindex gate: ShardReindexActivityLookup not yet installed; allowing backup. " +
						"Expected briefly during startup; if this persists past bootstrap, check the SetShardReindexActivityLookup wiring in configure_api.go.")
			})
			return
		}
		g.activity = activityBuilder()
		// Cleanup builder is optional. Memoizing it doesn't freeze the
		// answer: its closure reads a live registry on every call.
		if cleanupBuilder != nil {
			g.cleanup = cleanupBuilder()
		}
	})
}

// anyLiveReindexForShard reports whether the gate's snapshot blocks a
// backup of (collection, shardName).
func (g *reindexGate) anyLiveReindexForShard(collection, shardName string) bool {
	g.resolve()
	if g.activity == nil {
		return false
	}
	if g.activity(collection, shardName) {
		// Debug-level so flag-on operators get visibility into which
		// side of the OR fired the gate refusal. The matching cleanup
		// branch below logs at the same level.
		if g.db.logger != nil {
			g.db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "activity_lookup_live_task").
				Debug("backup-reindex gate: refusing — DTM lists a live reindex task on this shard")
		}
		return true
	}
	// Cleanup is OR-d in: the DTM task may have flipped to terminal
	// while autoCleanupAfterTerminal is still tearing the sidecar
	// buckets.
	if g.cleanup == nil {
		return false
	}
	if g.cleanup(collection, shardName) {
		if g.db.logger != nil {
			g.db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "cleanup_in_progress").
				Debug("backup-reindex gate: refusing — autoCleanupAfterTerminal still draining sidecars on this shard")
		}
		return true
	}
	return false
}

// AnyLiveReindexForShard answers the cluster-wide question: does DTM
// have any LIVE reindex task targeting (collection, shardName)? Each
// call resolves its own snapshot.
//
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go.
//
// Default to "no live reindex" when the lookup is unwired (with a
// one-time WARN). The original conservative default (refuse) was
// correct in isolation but broke every module-test fixture that
// spins up Weaviate without going through the post-bootstrap
// install path; production HTTP gates on bootstrap completion so the
// unwired window is unreachable by external traffic.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
	return newReindexGate(db).anyLiveReindexForShard(collection, shardName)
}

// SetReindexCleanupInProgressLookup installs the builder used by
// [DB.AnyLiveReindexForShard] to detect terminal-task cleanup that has
// not yet finished tearing __reindex / __ingest sidecar dirs. Wired in
// post-bootstrap alongside [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexCleanupInProgressLookup(builder CleanupInProgressLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexCleanupInProgressLookupBldr = builder
}

// refuseIfReindexInFlight is the per-shard backup-gate check for callers
// that are not a pass — [Shard.HaltForTransfer] reached by replica
// movement. Consults DTM via [DB.AnyLiveReindexForShard]; the
// filesystem-marker variant it replaced only saw the local node and
// lagged DTM's actual state.
//
// Each call resolves its own snapshot. Callers that walk many shards use
// [Index.refuseIfReindexInFlightWithGate] so the walk resolves once.
//
// If i.db is nil the gate is conservative: it refuses the backup, on
// the assumption that wiring is in progress.
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexInFlightError(i.Config.ClassName.String(), shardName, true)
	}
	if !i.db.AnyLiveReindexForShard(i.Config.ClassName.String(), shardName) {
		return nil
	}
	return reindexInFlightError(i.Config.ClassName.String(), shardName, false)
}

// refuseIfReindexInFlightWithGate is [Index.refuseIfReindexInFlight]
// answered from a caller-owned gate instead of a fresh one.
func (i *Index) refuseIfReindexInFlightWithGate(gate *reindexGate, shardName string) error {
	if i.db == nil {
		// Same conservative default as the fresh-snapshot form.
		return reindexInFlightError(i.Config.ClassName.String(), shardName, true)
	}
	if !gate.anyLiveReindexForShard(i.Config.ClassName.String(), shardName) {
		return nil
	}
	return reindexInFlightError(i.Config.ClassName.String(), shardName, false)
}

// reindexInFlightError formats the operator-facing rejection. The
// `preWire` flag distinguishes "DTM lookup says live" from "lookup not
// yet installed" so the error body can hint at the right next step.
func reindexInFlightError(collection, shardName string, preWire bool) error {
	if preWire {
		return fmt.Errorf(
			"%w: shard %q (collection %q): backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	}
	return fmt.Errorf(
		"%w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
	)
}

// NoSearchableIndexHint identifies which `PUT /v1/schema/{class}/indexes/{prop}`
// verb hit the "property has no searchable index" gate so the helper can
// emit the right remediation suggestion. Tokenization changes can fall
// back to the filterable side; rebuild and algorithm changes cannot.
type NoSearchableIndexHint int

const (
	// NoSearchableIndexHintTokenization is the hint for
	// `{"searchable":{"tokenization":...}}`: suggest the filterable
	// retokenization path as an alternative.
	NoSearchableIndexHintTokenization NoSearchableIndexHint = iota
	// NoSearchableIndexHintRebuildOrAlgorithm is the hint for
	// `{"searchable":{"rebuild":true}}` and
	// `{"searchable":{"algorithm":...}}`: only the enable-searchable
	// remediation makes sense (no filterable fallback).
	NoSearchableIndexHintRebuildOrAlgorithm
)

// NoSearchableIndexError formats the operator-facing 400 returned when
// a `PUT /v1/schema/{class}/indexes/{prop}` request asks the server to
// act on a searchable index that does not exist on the property. Centralised
// here so every handler call site emits identical phrasing — prior to
// unification three handlers used three slightly different strings
// ("has no searchable index; use ...", "does not have a searchable index",
// and the inline filterable hint), which made operator log triage harder
// and risked drift as new verbs were added.
//
// The canonical wording is "property %q has no searchable index" plus a
// verb-appropriate remediation tail; the inverse case ("already has a
// searchable index", emitted by enable-searchable validation) is
// deliberately not unified with this helper since it carries the
// opposite meaning.
func NoSearchableIndexError(propertyName string, hint NoSearchableIndexHint) string {
	switch hint {
	case NoSearchableIndexHintTokenization:
		return fmt.Sprintf(
			"property %q has no searchable index; use {\"filterable\":{\"tokenization\":...}} to retokenize the filterable bucket, or {\"searchable\":{\"enabled\":true,\"tokenization\":...}} to add a searchable index",
			propertyName,
		)
	default: // NoSearchableIndexHintRebuildOrAlgorithm
		return fmt.Sprintf(
			"property %q has no searchable index; use {\"searchable\":{\"enabled\":true,\"tokenization\":...}} to add one first",
			propertyName,
		)
	}
}
