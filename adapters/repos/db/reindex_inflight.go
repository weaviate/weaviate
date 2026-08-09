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
	"time"

	"github.com/sirupsen/logrus"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

// reindexGateSamplerBudget rate-limits each operator-facing gate WARN to one
// line per hour rather than once per shard checked.
//
// The unwired-gate window is reachable from outside: the lookup installs after
// /v1/.well-known/ready returns 200, so a backup POSTed on the first
// successful ready check can land inside it and be allowed without a gate
// check (reproduced on the first attempt in 40+ boots). The window widens with
// schema size and with reindex-scheduler work during bootstrap. That state
// persists, so the line has to keep reappearing for an operator who starts
// reading the logs later.
const reindexGateSamplerBudget = time.Hour

// reindexGateSamplers holds one budget per gate that samples a warning.
//
// Built per [DB], not per process: a package-level budget would leave every
// test after the first with an exhausted one.
type reindexGateSamplers struct {
	unwiredGate        *logrusext.Sampler
	unwiredRestoreGate *logrusext.Sampler
	unwiredOverlap     *logrusext.Sampler
	unknownHold        *logrusext.Sampler
	unreachableLeader  *logrusext.Sampler
}

func newReindexGateSamplers(logger logrus.FieldLogger) *reindexGateSamplers {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	newSampler := func() *logrusext.Sampler {
		return logrusext.NewSampler(logger, 1, reindexGateSamplerBudget)
	}
	return &reindexGateSamplers{
		unwiredGate:        newSampler(),
		unwiredRestoreGate: newSampler(),
		unwiredOverlap:     newSampler(),
		unknownHold:        newSampler(),
		unreachableLeader:  newSampler(),
	}
}

// gateSamplers returns this DB's samplers, building them on first use so the
// many fixtures that construct a bare &DB{} still get the production budget
// rather than a nil dereference.
func (db *DB) gateSamplers() *reindexGateSamplers {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	if db.reindexGateSamplers == nil {
		db.reindexGateSamplers = newReindexGateSamplers(db.logger)
	}
	return db.reindexGateSamplers
}

// warnUnwiredGate emits a fail-open gate WARN through sampler, which callers
// take from [DB.gateSamplers]. The sampler already carries the DB's logger.
func warnUnwiredGate(sampler *logrusext.Sampler, action, msg string) {
	sampler.WithSampling(func(l logrus.FieldLogger) {
		l.WithField("action", action).Warn(msg)
	})
}

// warnUnwiredReindexGate is [warnUnwiredGate] for the per-shard backup gate.
func (db *DB) warnUnwiredReindexGate() {
	warnUnwiredGate(db.gateSamplers().unwiredGate, "backup_reindex_gate",
		"backup-reindex gate: ShardReindexActivityLookup not yet installed; allowing backup. "+
			"Expected briefly during startup; if this persists past bootstrap, check the SetShardReindexActivityLookup wiring in configure_api.go.")
}

// warnUnknownReindexHold reports a hold kind the gate cannot classify.
func (db *DB) warnUnknownReindexHold(hold ReindexHold) {
	db.gateSamplers().unknownHold.WithSampling(func(l logrus.FieldLogger) {
		l.WithField("action", "backup_reindex_gate").
			WithField("hold", int(hold)).
			Warn("backup-reindex gate: refusing — unrecognized ReindexHold value. " +
				"A hold kind was added to the enum without teaching the backup gate about it; " +
				"add the missing case in reindexGate.blockReason.")
	})
}

// reindexBlockReason says which gate refused, so the refusal can give advice
// that matches: a cancelled task mid-teardown must not be described as one the
// operator can still cancel.
type reindexBlockReason int

const (
	reindexNotBlocked reindexBlockReason = iota
	reindexBlockedByLiveTask
	reindexBlockedByCleanup
	reindexBlockedBySubmit
	reindexBlockedPreWire
	// reindexBlockedByUnknownHold is the fail-closed answer for a
	// [ReindexHold] the gate cannot classify; see [reindexGate.blockReason].
	reindexBlockedByUnknownHold
)

// reindexGate is one pass's view of both backup-gate lookups. It resolves
// them at most once — the activity one is a leader-forwarded RAFT query,
// so per-shard rebuilds cost one round trip per shard — and judges every
// shard the pass checks against that one answer.
//
// Resolution is lazy: a pass that reaches no shard never queries. The
// admission pass ([DB.Backupable]) and the execution pass
// ([DB.BackupDescriptors]) each own one; the execution pass resolves up
// front and carries it in ctx, so the query happens before any shard's
// write-blocking backup lock is taken.
//
// Shards checked late in a pass therefore miss a task that appeared
// mid-pass. The pass was never atomic anyway, and
// [DB.RefuseIfReindexOverlapped] catches those at commit time.
type reindexGate struct {
	db       *DB
	once     sync.Once
	activity ShardReindexActivityLookup
	cleanup  CleanupInProgressLookup
	// unknown is set when the activity builder could not read
	// cluster-wide reindex state. The gate stays fail-closed, but the
	// refusal has to say that rather than claim a reindex it never saw.
	unknown error

	// refusalsMu guards refusals, which the capture pass fills from one
	// goroutine per shard.
	refusalsMu sync.Mutex
	// refusals maps a collection to the shards this pass refused; see
	// [reindexGate.logRefusals].
	refusals map[string][]string
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

// reindexGateFromContext returns nil outside a backup pass; replica movement
// resolves a fresh gate per call instead. Offload never reaches the gate at
// all.
func reindexGateFromContext(ctx context.Context) *reindexGate {
	gate, _ := ctx.Value(reindexGateCtxKey{}).(*reindexGate)
	return gate
}

func (g *reindexGate) resolve() {
	g.once.Do(func() {
		if g.db.config.RuntimeReindexDisabled {
			// No new task can start, so this gate checks nothing — which is
			// not the same as no task running; see
			// [DB.RefuseIfReindexOverlapped]. Returning before the builders run
			// is the point: the activity builder issues a leader-forwarded RAFT
			// query, and the kill switch has to cost nothing. Every gate
			// consumer resolves here, so this covers the capture path as well
			// as admission. An unresolved gate reads as "not blocked"
			// downstream, without the unwired warning below.
			//
			// Every entry point honours the flag itself rather than sharing one
			// check; grep RuntimeReindexDisabled and RuntimeReindexEnabled for
			// the full set. The others are [DB.RefuseIfAnyReindexInFlight],
			// [DB.RefuseIfReindexOverlapped], and the submission route in
			// adapters/handlers/rest, which stays open for cancel on purpose.
			// Together they make the flag-off behavior "no reindex check
			// anywhere", with one accepted residual, stated in full at
			// [DB.RefuseIfReindexOverlapped]. Pointed at rather than repeated:
			// a third copy of that enumeration is a drift hazard, and widening
			// it in only some copies is exactly how this comment ended up
			// narrower than the other two.
			return
		}

		// Read at resolve time: a gate built between the two install
		// calls must still see both builders, not just whichever landed
		// first.
		g.db.reindexAuditMu.RLock()
		activityBuilder := g.db.shardReindexActivityLookupBuilder
		cleanupBuilder := g.db.reindexCleanupInProgressLookupBldr
		g.db.reindexAuditMu.RUnlock()

		if activityBuilder == nil {
			g.db.warnUnwiredReindexGate()
		} else if activity, err := activityBuilder(); err != nil {
			g.unknown = err
			// Sampled like the other persistent-misconfiguration
			// warnings: an unreachable leader stays unreachable, and
			// the refusal body already tells the caller.
			g.db.gateSamplers().unreachableLeader.WithSampling(func(l logrus.FieldLogger) {
				l.WithField("action", "backup_reindex_gate").
					Warnf("backup-reindex gate: cannot read cluster-wide reindex state; refusing backups until the leader is reachable: %v", err)
			})
			return
		} else {
			g.activity = activity
		}
		// Read even when the activity builder is missing: the cleanup hold is a
		// local map read installed synchronously, before the goroutine that waits
		// for RAFT/DTM installs the activity builder, and suppressing it in the
		// meantime would hide an in-progress sidecar deletion from a concurrent
		// backup. Memoizing it does not freeze the answer — its closure reads a
		// live registry on every call.
		if cleanupBuilder != nil {
			g.cleanup = cleanupBuilder()
		}
	})
}

// noteRefusal records a shard this pass refused, for the summary
// [reindexGate.logRefusals] emits once the pass is done.
func (g *reindexGate) noteRefusal(collection, shardName string) {
	g.refusalsMu.Lock()
	defer g.refusalsMu.Unlock()
	if g.refusals == nil {
		g.refusals = map[string][]string{}
	}
	g.refusals[collection] = append(g.refusals[collection], shardName)
}

// logRefusals reports the shards this pass refused, one line per collection,
// through [DB.logReindexRefusals]. phase names the pass for the message.
//
// Emitted once at the end rather than per refusal: the capture pass checks
// shards in parallel, so a line per refusal would grow with shard count.
func (g *reindexGate) logRefusals(phase string) {
	g.refusalsMu.Lock()
	defer g.refusalsMu.Unlock()
	if len(g.refusals) == 0 || g.db == nil {
		return
	}
	g.db.logReindexRefusals(phase, g.db.localNodeName, g.refusals, nil)
}

// stateUnknownErr returns the pass-wide refusal to use when cluster-wide
// reindex state could not be read, and nil otherwise. One refusal covers
// the whole pass: no shard's state is known, so judging shards one by one
// would refuse every shard on the node while saying nothing true about
// any of them.
func (g *reindexGate) stateUnknownErr() error {
	g.resolve()
	if g.unknown == nil {
		return nil
	}
	return reindexStateUnknownError(g.unknown)
}

// stateUnknown reports whether an already-resolved gate failed to read
// cluster state. Unlike [reindexGate.stateUnknownErr] it does not
// resolve, so a caller that has not queried yet stays unqueried.
func (g *reindexGate) stateUnknown() bool {
	return g.unknown != nil
}

// anyLiveReindexForShard is [reindexGate.blockReason] as a yes/no.
// Unknown cluster state counts as blocked: the gate is fail-closed, and
// callers building an operator-facing refusal ask
// [reindexGate.stateUnknownErr] first so the message can say so.
func (g *reindexGate) anyLiveReindexForShard(collection, shardName string) bool {
	g.resolve()
	if g.unknown != nil {
		return true
	}
	return g.blockReason(collection, shardName) != reindexNotBlocked
}

// blockReason answers for one shard against the gate's resolved view,
// keeping the branch so the refusal can match its advice to what actually
// blocked.
//
// Unknown cluster state is not a reason: it is not a reindex, and callers
// building an operator-facing refusal ask [reindexGate.stateUnknownErr]
// first so the message says what really happened.
func (g *reindexGate) blockReason(collection, shardName string) reindexBlockReason {
	g.resolve()
	db := g.db
	if g.activity != nil && g.activity(collection, shardName) {
		// Debug-level so flag-on operators get visibility into which
		// side of the OR fired the gate refusal. The matching cleanup
		// branch below logs at the same level.
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "activity_lookup_live_task").
				Debug("backup-reindex gate: refusing — DTM lists a live reindex task on this shard")
		}
		return reindexBlockedByLiveTask
	}
	// Cleanup lookup is OR-d in: the DTM task may have flipped to
	// terminal while autoCleanupAfterTerminal is still tearing the
	// sidecar buckets.
	if g.cleanup == nil {
		return reindexNotBlocked
	}
	switch hold := g.cleanup(collection, shardName); hold {
	case ReindexHoldNone:
		return reindexNotBlocked
	case ReindexHoldCleanup:
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "cleanup_in_progress").
				Debug("backup-reindex gate: refusing — a cancelled task holds this shard, either tearing its sidecars down or waiting to start")
		}
		return reindexBlockedByCleanup
	case ReindexHoldSubmit:
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "submit_in_progress").
				Debug("backup-reindex gate: refusing — a reindex submission is sweeping stale sidecars on this shard")
		}
		return reindexBlockedBySubmit
	default:
		// Fail closed. Every arm above answers a hold this build knows how to
		// classify; anything else means the enum grew a kind the gate was never
		// taught, and guessing "not held" would admit a backup over a shard some
		// other operation is actively holding. Same direction as
		// [IsLiveReindexTaskStatus] on an unrecognized DTM status.
		db.warnUnknownReindexHold(hold)
		return reindexBlockedByUnknownHold
	}
}

// SetReindexCleanupInProgressLookup installs the builder used by
// [reindexGate] to detect terminal-task cleanup that has
// not yet finished tearing __reindex / __ingest sidecar dirs. Wired in
// post-bootstrap alongside [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexCleanupInProgressLookup(builder CleanupInProgressLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexCleanupInProgressLookupBldr = builder
}

// refuseIfReindexInFlight is the per-shard backup-gate check for callers
// that are not a pass (only [Shard.HaltForTransfer], for replica
// movement). Consults DTM; the filesystem-marker variant it replaced only
// saw the local node and lagged DTM's actual state.
//
// Resolves a fresh gate per call. Callers that walk many shards use
// [Index.refuseIfReindexInFlightWithGate] so the walk resolves once; see
// [reindexGate].
//
// If i.db is nil the gate is conservative: it refuses the backup, on the
// assumption that wiring is in progress.
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	if i.db == nil {
		return reindexInFlightError(i.Config.ClassName.String(), reindexBlockedPreWire)
	}
	err := i.refuseIfReindexInFlightWithGate(newReindexGate(i.db), shardName)
	if err != nil {
		i.logReindexRefusal(shardName)
	}
	return err
}

// refuseIfReindexInFlightWithGate is [Index.refuseIfReindexInFlight]
// answered from a caller-owned gate instead of a fresh one.
func (i *Index) refuseIfReindexInFlightWithGate(gate *reindexGate, shardName string) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexInFlightError(collection, reindexBlockedPreWire)
	}
	if err := gate.stateUnknownErr(); err != nil {
		return err
	}
	reason := gate.blockReason(collection, shardName)
	if reason == reindexNotBlocked {
		return nil
	}
	// Deliberately silent here: a multi-shard pass calls this once per shard,
	// so each caller logs at its own granularity instead
	// ([Index.refuseIfReindexInFlight], [Index.refuseIfReindexInFlightInPass],
	// [DB.Backupable]).
	return reindexInFlightError(collection, reason)
}

// refuseIfReindexInFlightInPass is [Index.refuseIfReindexInFlightWithGate] for
// the capture pass, which records the refused shard on the pass gate so
// [reindexGate.logRefusals] can name it at the end of the pass. Without that
// the shard reaches no one: the refusal body redacts it on purpose.
//
// An unknown cluster state is not recorded. It refuses every shard on the node
// without knowing anything about any of them, so listing them as held would
// claim a reindex the gate never saw; that case warns from
// [reindexGate.resolve] instead.
func (i *Index) refuseIfReindexInFlightInPass(gate *reindexGate, shardName string) error {
	err := i.refuseIfReindexInFlightWithGate(gate, shardName)
	if err != nil && !gate.stateUnknown() {
		gate.noteRefusal(i.Config.ClassName.String(), shardName)
	}
	return err
}

// logReindexRefusal records the shard the body withholds. Callers that check a
// single shard use this; a pass over many shards must summarise instead.
func (i *Index) logReindexRefusal(shardName string) {
	if i.db == nil || i.db.logger == nil {
		return
	}
	i.db.logger.WithField("action", "backup_reindex_gate").
		WithField("collection", i.Config.ClassName.String()).
		WithField("shard", shardName).
		WithField("node", i.db.localNodeName).
		Warn("backup-reindex gate: refused a backup; a runtime-reindex is live on this shard")
}

// reindexInFlightError formats the operator-facing rejection. reason picks the
// advice: a live task, a cancelled task still tearing down, and a lookup that
// is not yet installed each need a different next step.
//
// Names no shard and no node: this text reaches an API response body, and
// backing up a collection grants nothing on either. The caller already named
// the collection, and the shard and node reach the operator through the log in
// [Index.refuseIfReindexInFlight].
func reindexInFlightError(collection string, reason reindexBlockReason) error {
	var advice string
	switch reason {
	case reindexBlockedPreWire:
		advice = ": backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping"
	case reindexBlockedBySubmit:
		// Nothing was cancelled here, so the cleanup text below would send the
		// operator looking for a migration that does not exist.
		advice = ": a reindex submission is preparing this collection; retry in a moment"
	case reindexBlockedByUnknownHold:
		// The gate knows the shard is held but not by what, so this promises
		// nothing it cannot back: no cancelled migration, no submission, no
		// duration estimate. The diagnosis is a server-side defect, which is
		// what the log line in [reindexGate.blockReason] carries.
		advice = " is held by a reindex operation this server build does not recognize; retry, and report this to Weaviate if it persists"
	case reindexBlockedByCleanup:
		// No cancel advice: the task this is cleaning up after is already
		// cancelled, and telling the operator to cancel it sends them looking
		// for something that is gone.
		advice = ": a cancelled migration is still removing its temporary index files; retry once the cleanup finishes (usually a few seconds)"
	default:
		// Cancel is conditional: DTM only accepts it pre-commit (PREPARING and
		// SWAPPING refuse it too), so promising it outright loops operators
		// between a refused backup and a no-op cancel. Waiting is not
		// guaranteed to end either — a node owning part of the task leaving
		// the cluster wedges it past STARTED for good; only a restart with the
		// flag off lifts that.
		advice = " has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\"). While it is still building indexes you can cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}; once it has started committing its result it can only be waited out, and if a node that owned part of it left the cluster it never finishes at all — a restart with RUNTIME_REINDEX_ENABLED=false is then the only way to lift this refusal. If every index already reports \"ready\", the task holding this gate is one this server cannot attribute to a collection — the same cancel call, on any collection, clears it"
	}
	return entitiesbackup.ReindexBlockedError{Msg: fmt.Sprintf("%s: collection %q%s",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, collection, advice)}
}

// reindexStateUnknownError is the refusal for "the cluster leader could
// not be reached": it names no shard and suggests cancelling nothing,
// replacing what used to be a refusal per shard (a 7 MB body on a
// 20,000-shard node).
func reindexStateUnknownError(cause error) error {
	return reindexStateUnknown{
		ReindexBlockedError: entitiesbackup.ReindexBlockedError{Msg: "backup blocked: the cluster leader " +
			"could not be reached, so runtime-reindex state is unknown for every shard on this node; " +
			"refusing the backup rather than risk snapshotting a shard mid-reindex. Retry once the leader is reachable"},
		cause: cause,
	}
}

// reindexStateUnknown is a type rather than a wrapped sentinel because
// `%w` renders the sentinel first, and the sentinel reads "runtime-reindex
// in flight" — the exact claim this refusal exists to stop making. A
// caller reading the first line of the response would take it as fact.
//
// The cause is carried but not printed: it is a RAFT-transport error, and
// backing up a collection grants nothing on cluster internals. It reaches
// the operator through the gate's WARN, and errors.Is through Unwrap.
// Unwrapping the embedded [entitiesbackup.ReindexBlockedError] keeps the
// message publishable in the stored failure meta.
type reindexStateUnknown struct {
	entitiesbackup.ReindexBlockedError
	cause error
}

func (e reindexStateUnknown) Unwrap() []error {
	// ErrReindexStateUnknown is the marker the canCommit boundary reads to
	// keep this refusal apart from a genuine one.
	unwrapped := []error{e.ReindexBlockedError, entitiesbackup.ErrReindexStateUnknown}
	if e.cause == nil {
		return unwrapped
	}
	return append(unwrapped, e.cause)
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
