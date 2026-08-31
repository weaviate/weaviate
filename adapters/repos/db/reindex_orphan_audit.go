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
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// KnownReindexTaskLookup reports whether (taskID, taskVersion) is live
// in the DTM scheduler snapshot. One instance is built per audit
// invocation so all per-tracker classifications share a consistent
// snapshot.
type KnownReindexTaskLookup func(taskID string, taskVersion uint64) bool

// KnownReindexTaskLookupBuilder returns a fresh [KnownReindexTaskLookup]
// for one audit invocation. Returns an error when the underlying DTM
// snapshot cannot be obtained (e.g. ListDistributedTasks is timing out
// during a network partition). Callers MUST propagate the error rather
// than substitute a soft default — an unobservable "all known" fallback
// would silently misclassify orphans as in-flight migrations.
type KnownReindexTaskLookupBuilder func() (KnownReindexTaskLookup, error)

// NewLiveReindexTrackerLookup snapshots which tasks still own their
// on-disk tracker dirs. The rule lives in [IsLiveReindexTaskStatus] so a
// new status has one place to answer it.
func NewLiveReindexTrackerLookup(tasks []*distributedtask.Task) KnownReindexTaskLookup {
	type taskKey struct {
		id      string
		version uint64
	}
	live := make(map[taskKey]bool, len(tasks))
	for _, task := range tasks {
		live[taskKey{task.ID, task.Version}] = IsLiveReindexTaskStatus(task.Status)
	}
	return func(taskID string, taskVersion uint64) bool {
		return live[taskKey{taskID, taskVersion}]
	}
}

// AuditOutcomeStatus distinguishes the three operationally distinct
// reasons an [DB.AuditOrphanReindexTrackers] invocation can return
// without an error. Closes S4 (collapsed three outcomes into one
// silent nil): callers and operators now have a typed signal for
// "audit sweep ran" vs. "audit deferred" vs. "audit ran but some
// per-tracker cleanups failed".
type AuditOutcomeStatus int

const (
	// AuditStatusSkipped: deps not installed, root path empty, or
	// root path unreadable. No tracker dirs were inspected. The
	// audit's startup retry path is expected to retry later.
	AuditStatusSkipped AuditOutcomeStatus = iota
	// AuditStatusRan: the sweep traversed every shard under
	// RootPath and inspected every .migrations tracker. No orphans
	// found AND no per-tracker errors. The expected steady-state
	// outcome.
	AuditStatusRan
	// AuditStatusOrphansFound: the sweep ran end-to-end and at
	// least one orphan tracker was identified and successfully
	// cleaned.
	AuditStatusOrphansFound
	// AuditStatusPartialFail: the sweep ran end-to-end but at
	// least one per-tracker cleanup failed (e.g. PauseCompaction
	// timed out, os.RemoveAll returned EACCES). Other trackers may
	// have been cleaned successfully. Operators must investigate;
	// the next process restart will retry the failed dirs.
	AuditStatusPartialFail
)

// String returns the stable, lower-snake-case label used in logs and
// (in the future) metrics labels.
func (s AuditOutcomeStatus) String() string {
	switch s {
	case AuditStatusSkipped:
		return "skipped"
	case AuditStatusRan:
		return "ran"
	case AuditStatusOrphansFound:
		return "orphans_found"
	case AuditStatusPartialFail:
		return "partial_fail"
	}
	return "unknown"
}

// AuditOutcome is the typed result returned by
// [DB.AuditOrphanReindexTrackers] and the no-arg wrapper
// [DB.AuditOrphanReindexTrackersIfReady]. Every successful invocation
// emits one Info-level log line with these counters so absence of the
// line is detectable in operator logs (S4 fix).
type AuditOutcome struct {
	Status       AuditOutcomeStatus
	ScannedCount int
	OrphansFound int
	OrphansClean int
	FailedDirs   []string
	SkipReason   string
}

// SetReindexAuditDeps installs the builder and logger used by
// [DB.AuditOrphanReindexTrackersIfReady]. If
// [DB.AuditOrphanReindexTrackersIfReady] was invoked one or more times
// before this call (race between RAFT replay firing per-class-dir
// restores and the Scheduler.Start goroutine that installs deps), the
// counter is non-zero and a single replay sweep runs synchronously so
// the deferred audit work is not silently lost. Closes B2.
//
// The deferred-replay path runs with [context.Background]; it does not
// inherit the caller's context. A caller-side cancellation that needs to
// abort an in-flight replay must wait for [PauseCompaction]'s internal
// timeout. Switching to a caller-supplied context would let SIGTERM /
// shutdown abort the replay cleanly, but the current shape — fire-and-
// forget background — matches the post-bootstrap goroutine that calls
// us in production.
func (db *DB) SetReindexAuditDeps(builder KnownReindexTaskLookupBuilder, logger logrus.FieldLogger) {
	db.reindexAuditMu.Lock()
	db.reindexAuditLookupBuilder = builder
	db.reindexAuditLogger = logger
	deferred := db.reindexAuditDeferredRequests
	db.reindexAuditDeferredRequests = 0
	db.reindexAuditMu.Unlock()

	if deferred == 0 || builder == nil {
		return
	}
	replayLogger := logger
	if replayLogger == nil {
		replayLogger = logrus.New()
	}
	replayLogger.WithField("action", "reindex_orphan_audit").
		WithField("deferred_requests", deferred).
		Info("reindex orphan audit: replaying audits requested before deps were installed (B2 race window)")
	lookup, buildErr := builder()
	if buildErr != nil {
		replayLogger.WithField("action", "reindex_orphan_audit").
			Errorf("reindex orphan audit: deferred-replay builder failed; the next process restart will retry: %v", buildErr)
		return
	}
	if _, err := db.AuditOrphanReindexTrackers(context.Background(), lookup, logger); err != nil {
		replayLogger.WithField("action", "reindex_orphan_audit").
			Warnf("reindex orphan audit: deferred-replay sweep returned an error; the next process restart will retry: %v", err)
	}
}

// AuditOrphanReindexTrackersIfReady is the no-arg wrapper for callers
// without the lookup builder in scope. The post-restore hook lives in
// `adapters/handlers/rest/configure_api.go`'s `restoreClassDirWithAudit`
// closure (around line 711) which wraps `backup.RestoreClassDir`.
// Returns an outcome with Status==Skipped when deps are not yet
// installed; the deferred-request counter is incremented so
// [DB.SetReindexAuditDeps] replays the audit when deps land. A WARN
// log is emitted on the skip path so the no-op is detectable. Closes B2.
func (db *DB) AuditOrphanReindexTrackersIfReady(ctx context.Context) (AuditOutcome, error) {
	db.reindexAuditMu.Lock()
	builder := db.reindexAuditLookupBuilder
	logger := db.reindexAuditLogger
	if builder == nil {
		db.reindexAuditDeferredRequests++
		deferredNow := db.reindexAuditDeferredRequests
		db.reindexAuditMu.Unlock()
		warnLogger := logger
		if warnLogger == nil {
			warnLogger = logrus.New()
		}
		warnLogger.WithField("action", "reindex_orphan_audit").
			WithField("deferred_requests", deferredNow).
			Warn("reindex orphan audit: deps not yet installed; deferring audit until SetReindexAuditDeps lands. " +
				"If this WARN persists past process startup, the install path is broken.")
		return AuditOutcome{
			Status:     AuditStatusSkipped,
			SkipReason: "deps_not_installed",
		}, nil
	}
	db.reindexAuditMu.Unlock()
	lookup, buildErr := builder()
	if buildErr != nil {
		warnLogger := logger
		if warnLogger == nil {
			warnLogger = logrus.New()
		}
		warnLogger.WithField("action", "reindex_orphan_audit").
			Errorf("reindex orphan audit: lookup builder failed; skipping this invocation: %v", buildErr)
		return AuditOutcome{
			Status:     AuditStatusSkipped,
			SkipReason: "builder_error",
		}, fmt.Errorf("reindex orphan audit: lookup builder failed: %w", buildErr)
	}
	return db.AuditOrphanReindexTrackers(ctx, lookup, logger)
}

// orphanReindexTracker carries the fields the cleanup loop logs and
// acts on per orphan.
type orphanReindexTracker struct {
	collection  string
	shardName   string
	dirName     string
	prefix      string
	generation  int
	taskID      string
	taskVersion uint64
	unitID      string
	properties  []string
	indexTypes  []string
}

// String formats one keyed line per field for greppable log queries.
func (o *orphanReindexTracker) String() string {
	return fmt.Sprintf(
		"collection=%q shard=%q tracker=%q gen=%d taskID=%q taskVersion=%d unitID=%q properties=%v indexTypes=%v",
		o.collection, o.shardName, o.dirName, o.generation,
		o.taskID, o.taskVersion, o.unitID, o.properties, o.indexTypes)
}

// AuditOrphanReindexTrackers quarantines .migrations/<tracker>/ dirs
// whose payload.mig references a (TaskID, TaskVersion) the DTM
// scheduler does not know about (typical: restored cluster whose
// backup captured the tracker but not the DTM unit driving it).
// Calls [Shard.CleanStalePartialReindexState] per (property, indexType)
// for loaded shards; cold lazy MT shards are skipped and re-evaluated
// at next activation. Best-effort: per-orphan errors are logged.
//
// Returns a typed [AuditOutcome] so callers can distinguish "audit
// skipped because deps missing" from "audit ran and found N orphans"
// from "audit ran but K cleanups failed". The outcome is also logged
// at Info level on every successful invocation (S4 fix: absence of
// that log line in operator dashboards is now detectable).
func (db *DB) AuditOrphanReindexTrackers(ctx context.Context, knownTask KnownReindexTaskLookup, logger logrus.FieldLogger) (AuditOutcome, error) {
	if logger == nil {
		logger = logrus.New()
	}
	auditLogger := logger.WithField("action", "reindex_orphan_audit")
	if knownTask == nil {
		// A nil lookup would misclassify every in-flight migration as an
		// orphan. Refuse rather than auto-quarantine on a normal restart.
		auditLogger.Error("reindex orphan audit: KnownReindexTaskLookup is nil; skipping audit")
		return AuditOutcome{Status: AuditStatusSkipped, SkipReason: "nil_lookup"},
			fmt.Errorf("reindex orphan audit: KnownReindexTaskLookup is nil")
	}

	rootPath := db.config.RootPath
	if rootPath == "" {
		auditLogger.Warn("reindex orphan audit: RootPath empty; skipping audit. This should not happen in steady-state; check DB.config wiring.")
		return AuditOutcome{Status: AuditStatusSkipped, SkipReason: "empty_root_path"}, nil
	}

	indexEntries, err := os.ReadDir(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			auditLogger.WithField("path", rootPath).
				Info("reindex orphan audit: root path does not exist; skipping audit (no shards on disk)")
			return AuditOutcome{Status: AuditStatusSkipped, SkipReason: "root_path_missing"}, nil
		}
		auditLogger.WithField("path", rootPath).
			Warnf("reindex orphan audit: cannot read root path; skipping audit: %v", err)
		return AuditOutcome{Status: AuditStatusSkipped, SkipReason: "root_path_unreadable"}, nil
	}

	// Snapshot loaded indexes so per-shard cleanup can route through
	// in-memory state when the shard is loaded.
	loadedByID := func() map[string]*Index {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()
		snapshot := make(map[string]*Index, len(db.indices))
		for id, idx := range db.indices {
			snapshot[id] = idx
		}
		return snapshot
	}()

	outcome := AuditOutcome{Status: AuditStatusRan}
	for _, indexEntry := range indexEntries {
		if !indexEntry.IsDir() {
			continue
		}
		indexDir := indexEntry.Name()
		indexPath := filepath.Join(rootPath, indexDir)
		shardEntries, shardErr := os.ReadDir(indexPath)
		if shardErr != nil {
			continue
		}
		idx := loadedByID[indexDir]
		// Hold idx.dropIndex.RLock while reading idx.Config / idx.shards
		// to prevent a concurrent Drop/DeleteClass from tearing the
		// Index down underneath the audit.
		processIndex := func() {
			// Loaded-index branch uses the real class name; unloaded
			// fallback uses the on-disk dir name.
			collection := indexDir
			if idx != nil {
				idx.dropIndex.RLock()
				defer idx.dropIndex.RUnlock()
				if idx.Config.ClassName != "" {
					collection = idx.Config.ClassName.String()
				}
			}
			for _, shardEntry := range shardEntries {
				if !shardEntry.IsDir() {
					continue
				}
				shardName := shardEntry.Name()
				lsmPath := filepath.Join(indexPath, shardName, "lsm")
				outcome.ScannedCount++
				orphans := collectOrphanTrackers(lsmPath, collection, shardName, knownTask, auditLogger)
				if len(orphans) == 0 {
					// No orphans this sweep: clear any stale quarantine
					// sentinels — a tracker that flipped back to "known
					// live" between sweeps must not retain its quarantine
					// or a subsequent legitimately-orphan sweep would
					// immediately destroy it.
					clearStaleQuarantineSentinels(lsmPath, knownTask, auditLogger)
					continue
				}
				outcome.OrphansFound += len(orphans)

				// S2: gate destructive cleanup on a quarantine window.
				// On first detection, partitionOrphansByQuarantine writes
				// audit_quarantined.mig into each tracker dir and returns
				// only the orphans whose sentinel is older than
				// reindexAuditQuarantineWindow. A stale single-node DTM
				// snapshot (typical on a freshly-rejoined follower)
				// loses by default: the audit emits a WARN, sleeps the
				// quarantine window, and re-evaluates with fresh DTM
				// state on the next sweep.
				confirmed := partitionOrphansByQuarantine(lsmPath, orphans, auditLogger)
				if len(confirmed) == 0 {
					continue
				}
				var shard *Shard
				if idx != nil {
					if sl := idx.shards.Load(shardName); sl != nil {
						if s, ok := sl.(*Shard); ok {
							shard = s
						}
					}
				}
				var cleaned int
				var failed []string
				if shard != nil {
					cleaned, failed = db.cleanLoadedShardOrphans(ctx, shard, confirmed, auditLogger)
				} else {
					cleaned, failed = db.cleanUnloadedShardOrphans(lsmPath, confirmed, auditLogger)
				}
				outcome.OrphansClean += cleaned
				outcome.FailedDirs = append(outcome.FailedDirs, failed...)
			}
		}
		processIndex()
	}

	if len(outcome.FailedDirs) > 0 {
		outcome.Status = AuditStatusPartialFail
	} else if outcome.OrphansFound > 0 {
		outcome.Status = AuditStatusOrphansFound
	}

	// Single canonical Info log emitted on every successful audit
	// sweep. Absence of this line in operator logs is the detection
	// signal for "audit silently skipped".
	auditLogger.
		WithField("status", outcome.Status.String()).
		WithField("scanned_count", outcome.ScannedCount).
		WithField("orphans_found", outcome.OrphansFound).
		WithField("orphans_cleaned", outcome.OrphansClean).
		WithField("failed_dirs", len(outcome.FailedDirs)).
		Infof("reindex orphan audit: complete (status=%s scanned=%d orphans=%d cleaned=%d failed=%d)",
			outcome.Status, outcome.ScannedCount, outcome.OrphansFound,
			outcome.OrphansClean, len(outcome.FailedDirs))
	return outcome, nil
}

// reindexAuditQuarantineFile is the sentinel file the audit writes
// into a tracker dir on first orphan detection (S2 quarantine).
// Subsequent audits read its mtime to gate the destructive cleanup
// behind the quarantine window. The concrete name lives here so it stays
// distinct from the tracker payload the audit reads beside it.
const reindexAuditQuarantineFile = "audit_quarantined.mig"

// reindexAuditQuarantineWindow is the minimum age (mtime) of
// audit_quarantined.mig before the next audit sweep is allowed to
// commit to destructive cleanup. 5 minutes is the balance point: long
// enough for a freshly-rejoined follower to catch up RAFT and observe
// the leader's full DTM state via the next audit's
// ListDistributedTasks; short enough that operators expecting
// post-restore audit cleanup do not see indefinite delay.
//
// On a follower that mis-classified a live migration as orphan due to
// stale RAFT (S2), the second audit will see the same tracker but with
// fresh DTM state from the leader → classification flips to "known
// live" → quarantine sentinel is removed without destruction.
const reindexAuditQuarantineWindow = 5 * time.Minute

// collectOrphanTrackers walks <lsmPath>/.migrations/ and returns every tracker
// dir classified as an orphan: a migration record names it, its data is not
// committed, and the task that record names is not known to DTM. Read-only;
// cleanup is the caller's job.
//
// A directory no record names is the second kind of orphan, and this is its
// only reclaimer: a run that crashed before its buckets opened has a live
// task but no record, so its identity comes from payload.mig instead. A
// missing payload gives up its directory alone; an unreadable one is left
// entirely, since only absence proves the property list is empty. Age
// separates all of these from a directory this process is still writing.
func collectOrphanTrackers(lsmPath, collection, shardName string, knownTask KnownReindexTaskLookup, logger logrus.FieldLogger) []orphanReindexTracker {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		return nil
	}
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	if someRecordsUnreadable || recordSetUnreadable {
		// A record this build cannot read may name any tracker here, and the
		// record-less fallback only has payload.mig, which a tracker whose
		// write was torn need not carry. So this withholds reclamation shard-wide, like every
		// other consumer of an unreadable record set — costing disk, not data.
		logger.WithField("collection", collection).WithField("shard", shardName).
			Warn("reindex orphan audit: migration records could not be read; reclaiming nothing on this shard")
		return nil
	}
	var orphans []orphanReindexTracker
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		prefix, generation, ok := parseMigrationDirName(dirName)
		if !ok {
			continue
		}
		trackerPath := filepath.Join(migsDir, dirName)
		rec, ok := migrationRecordForTracker(records, dirName)
		if !ok {
			old, mtime, err := migrationDirPredatesThisProcess(trackerPath)
			if err != nil {
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).
					Warnf("reindex orphan audit: tracker names no migration record and its mtime is unreadable; manual cleanup may be needed: %v", err)
				continue
			}
			if !old {
				// The record is written later in the same run, once the
				// migration's buckets are open, so a directory this process
				// just created may not have one yet.
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).WithField("tracker_mtime", mtime).
					Warn("reindex orphan audit: tracker names no migration record but was created after this process started; leaving it for the next sweep")
				continue
			}
			// payload.mig, not the record, is what a tracker whose run
			// crashed before its record landed still has. Without its
			// property list the cleanup removes the tracker and leaves the
			// sidecar directories behind, and the next migration re-issues
			// the same generation straight onto them.
			payload, _ := readTaskProps(filepath.Join(migsDir, dirName))
			if payload.unreadable {
				// Present but unreadable is not the same as absent, and only
				// absent means "there were no properties". Reclaiming on a
				// payload nobody could parse would delete the tracker on the
				// strength of a list we never saw.
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).WithField("tracker_mtime", mtime).
					Warn("reindex orphan audit: tracker names no migration record and its payload could not be read; reclaiming nothing for it")
				continue
			}
			if knownTask(payload.taskID, payload.taskVersion) {
				// The record is written after the migration's buckets open, so
				// a tracker whose run crashed before that has none while its
				// task is still live and about to resume. payload.mig is
				// written before the iteration and carries the identity that
				// answers.
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).WithField("taskID", payload.taskID).
					Info("reindex orphan audit: tracker names no migration record but its task is still live; leaving it alone")
				continue
			}
			indexTypes, known := semanticMigrationIndexTypesForAudit(payload.migrationType)
			if !known {
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).WithField("migration_type", payload.migrationType).
					Warn("reindex orphan audit: tracker names a migration type this build does not know, " +
						"so what it owns cannot be composed; reclaiming nothing for it")
				continue
			}
			logger.WithField("collection", collection).WithField("shard", shardName).
				WithField("tracker", dirName).WithField("tracker_mtime", mtime).
				WithField("props", payload.props).
				Warn("reindex orphan audit: tracker predating this process names no migration record; quarantining it as a class-level orphan")
			orphans = append(orphans, orphanReindexTracker{
				collection:  collection,
				shardName:   shardName,
				dirName:     dirName,
				prefix:      prefix,
				generation:  generation,
				taskID:      payload.taskID,
				taskVersion: payload.taskVersion,
				unitID:      payload.unitID,
				properties:  append([]string(nil), payload.props...),
				indexTypes:  indexTypes,
			})
			continue
		}
		if rec.StagedDataComplete() {
			continue
		}
		subject := rec.Subject()
		if knownTask(subject.TaskID, subject.Key.TaskVersion) {
			continue
		}
		// The decoder refuses a record naming a type this build does not know,
		// so the second result cannot be false here.
		indexTypes, _ := semanticMigrationIndexTypesForAudit(subject.MigrationType)
		orphans = append(orphans, orphanReindexTracker{
			collection:  collection,
			shardName:   shardName,
			dirName:     dirName,
			prefix:      prefix,
			generation:  generation,
			taskID:      subject.TaskID,
			taskVersion: subject.Key.TaskVersion,
			unitID:      subject.Key.UnitID,
			properties:  append([]string(nil), subject.Properties()...),
			indexTypes:  indexTypes,
		})
	}
	return orphans
}

// processStartTime is the clock the record-less arm of collectOrphanTrackers
// compares against. The audit runs many times per process, but the question it
// answers is fixed: was this directory on disk before this process existed?
// Only the first-boot timestamp can answer that.
var processStartTime = time.Now()

// migrationDirPredatesThisProcess reports whether a tracker directory that no
// record names was already on disk when this process started.
//
// A directory carrying a quarantine sentinel answers from the sentinel's
// presence instead: an earlier sweep already found no record for it, and the
// age question does not reset once asked.
func migrationDirPredatesThisProcess(trackerPath string) (bool, time.Time, error) {
	info, err := os.Stat(trackerPath)
	if err != nil {
		return false, time.Time{}, err
	}
	mtime := info.ModTime()
	if fileExists(filepath.Join(trackerPath, reindexAuditQuarantineFile)) {
		return true, mtime, nil
	}
	return !mtime.After(processStartTime), mtime, nil
}

// partitionOrphansByQuarantine passes through only the orphans whose
// audit_quarantined.mig has been on disk for [reindexAuditQuarantineWindow];
// the rest get a sentinel written and wait for a later sweep. One node's DTM
// snapshot can be stale — a follower that has not caught up reports a live task
// as gone — so the second sweep runs against fresh state and either confirms
// the orphan or clears the sentinel through [clearStaleQuarantineSentinels].
//
// A sentinel this cannot stat or write passes the orphan straight through, on
// the view that a permanently broken disk path is worse than a missed
// quarantine. The log line names which happened.
func partitionOrphansByQuarantine(lsmPath string, orphans []orphanReindexTracker, logger logrus.FieldLogger) []orphanReindexTracker {
	confirmed := make([]orphanReindexTracker, 0, len(orphans))
	for i := range orphans {
		o := &orphans[i]
		trackerPath := filepath.Join(lsmPath, ".migrations", o.dirName)
		sentinelPath := filepath.Join(trackerPath, reindexAuditQuarantineFile)
		info, err := os.Stat(sentinelPath)
		switch {
		case err == nil:
			age := time.Since(info.ModTime())
			if age >= reindexAuditQuarantineWindow {
				logger.WithField("orphan", o.String()).
					WithField("quarantine_age", age.String()).
					Warn("reindex orphan audit: quarantine window elapsed; confirming destructive cleanup")
				confirmed = append(confirmed, *o)
			} else {
				logger.WithField("orphan", o.String()).
					WithField("quarantine_age", age.String()).
					WithField("quarantine_window", reindexAuditQuarantineWindow.String()).
					Warn("reindex orphan audit: orphan still inside quarantine window; deferring cleanup to next audit sweep")
			}
		case os.IsNotExist(err):
			if writeErr := writeQuarantineSentinel(trackerPath); writeErr != nil {
				// Disk write failed. Pass through to cleanup with a
				// distinct WARN so the destructive path is traceable
				// to the missed quarantine.
				logger.WithField("orphan", o.String()).
					Warnf("reindex orphan audit: could not write quarantine sentinel; falling back to immediate destructive cleanup: %v", writeErr)
				confirmed = append(confirmed, *o)
				continue
			}
			logger.WithField("orphan", o.String()).
				WithField("quarantine_window", reindexAuditQuarantineWindow.String()).
				Warn("reindex orphan audit: orphan tracker detected; quarantining for second-sweep confirmation before destructive cleanup")
		default:
			// Sentinel stat failed with a non-ENOENT error (EACCES,
			// EIO, etc.). Pass through to cleanup with a WARN — same
			// rationale as the writeErr branch.
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: could not stat quarantine sentinel; falling back to immediate destructive cleanup: %v", err)
			confirmed = append(confirmed, *o)
		}
	}
	return confirmed
}

// preserveTrackerDirMtime returns the restore for trackerPath's own
// modification time. A tracker no record names is classified by that mtime and
// the audit is its only reclaimer, so a sweep that moves it strands the tracker
// for the rest of the process. Best effort: a crash before the restore leaves
// the directory looking fresh, which is where it was without this.
func preserveTrackerDirMtime(trackerPath string) func() {
	info, err := os.Stat(trackerPath)
	if err != nil {
		return func() {}
	}
	return func() { _ = os.Chtimes(trackerPath, time.Now(), info.ModTime()) }
}

// removeQuarantineSentinel clears the sentinel without moving the directory
// mtime the audit classifies by.
func removeQuarantineSentinel(trackerPath string) error {
	defer preserveTrackerDirMtime(trackerPath)()
	return os.Remove(filepath.Join(trackerPath, reindexAuditQuarantineFile))
}

// writeQuarantineSentinel creates audit_quarantined.mig in trackerPath
// with the current time as mtime. The file's mtime is the
// authoritative timestamp the next audit compares against
// reindexAuditQuarantineWindow.
func writeQuarantineSentinel(trackerPath string) error {
	defer preserveTrackerDirMtime(trackerPath)()
	sentinelPath := filepath.Join(trackerPath, reindexAuditQuarantineFile)
	f, err := os.OpenFile(sentinelPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		// EEXIST is benign — a concurrent audit may have written it.
		if os.IsExist(err) {
			return nil
		}
		return fmt.Errorf("create quarantine sentinel %q: %w", sentinelPath, err)
	}
	return f.Close()
}

// clearStaleQuarantineSentinels removes audit_quarantined.mig from tracker dirs
// whose record — or, for a tracker that has none yet, whose payload — maps to
// a known-live DTM task. Called per-shard
// when the orphan list is empty, so that a sweep which mis-classified a live
// migration as an orphan (a follower with stale RAFT, say) does not leave a
// quarantine age behind for a future, legitimate orphan to inherit.
//
// Errors are logged at Warn and never propagated: the worst case is a stale
// sentinel that turns a later-detected orphan into an immediate destructive
// cleanup, and at that point the orphan was real and the cleanup is logged.
func clearStaleQuarantineSentinels(lsmPath string, knownTask KnownReindexTaskLookup, logger logrus.FieldLogger) {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		return
	}
	// Most shards carry no sentinel at all, and this runs per shard on every
	// sweep, so the record store is only read once one is found.
	var quarantined []string
	for _, entry := range entries {
		if entry.IsDir() && fileExists(filepath.Join(migsDir, entry.Name(), reindexAuditQuarantineFile)) {
			quarantined = append(quarantined, entry.Name())
		}
	}
	if len(quarantined) == 0 {
		return
	}
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	for _, dirName := range quarantined {
		trackerPath := filepath.Join(migsDir, dirName)
		if someRecordsUnreadable || recordSetUnreadable {
			// A matured sentinel is stored destructive intent. Leaving it on a
			// shard nothing can classify means the first sweep after the
			// records read again deletes with no fresh grace period, so clear
			// it and let that sweep start the window over.
			if rmErr := removeQuarantineSentinel(trackerPath); rmErr != nil && !os.IsNotExist(rmErr) {
				logger.WithField("tracker", dirName).
					Warnf("reindex orphan audit: failed to clear quarantine sentinel on a shard whose records could not be read: %v", rmErr)
			}
			continue
		}
		rec, recOK := migrationRecordForTracker(records, dirName)
		if !recOK {
			// The record lands once the migration's buckets open, so a run
			// that crashed before that carries only payload.mig — the same
			// identity collectOrphanTrackers classifies it by. Reading it here
			// too is what makes the grace window repeatable instead of
			// single-use: without it the sentinel this tracker already carries
			// can never be cleared, and the next sweep that misclassifies it
			// destroys on the first pass.
			payload, _ := readTaskProps(trackerPath)
			if payload.unreadable || !knownTask(payload.taskID, payload.taskVersion) {
				continue
			}
		} else if !knownTask(rec.Subject().TaskID, rec.Subject().Key.TaskVersion) {
			// Tracker is still classified as orphan from the record's
			// perspective; the empty-orphans branch got here only because
			// collectOrphanTrackers already filtered upstream (e.g. the
			// migration has since committed). Either way the sentinel is now
			// load-bearing for a future orphan sweep, so leave it alone.
			continue
		}
		if rmErr := removeQuarantineSentinel(trackerPath); rmErr != nil && !os.IsNotExist(rmErr) {
			logger.WithField("tracker", dirName).
				Warnf("reindex orphan audit: failed to clear stale quarantine sentinel after task flipped back to known-live: %v", rmErr)
		}
	}
}

// cleanLoadedShardOrphans cleans every orphan on the shard under a
// single PauseCompaction window. A per-orphan pause/resume cycle would
// race the cycle manager: the resume between orphans lets a fresh
// compaction start on the next sidecar bucket, and the next pause
// times out trying to drain it.
//
// Returns (cleanedCount, failedDirs) so the audit driver can roll up
// per-shard results into the typed [AuditOutcome] (S4).
func (db *DB) cleanLoadedShardOrphans(ctx context.Context, shard *Shard, orphans []orphanReindexTracker, logger logrus.FieldLogger) (int, []string) {
	if len(orphans) == 0 {
		return 0, nil
	}
	pauseCtx, cancelPause := context.WithTimeout(ctx, orphanCleanupPauseTimeout)
	defer cancelPause()
	if err := shard.store.PauseCompaction(pauseCtx); err != nil {
		logger.WithField("collection", orphans[0].collection).WithField("shard", orphans[0].shardName).
			Warnf("reindex orphan audit: failed to pause compaction; skipping shard cleanup: %v", err)
		// Every orphan on this shard counts as a failed cleanup so the
		// outcome captures the shard's missed work.
		failed := make([]string, 0, len(orphans))
		for i := range orphans {
			failed = append(failed, orphans[i].dirName)
		}
		return 0, failed
	}
	// Resume must fire even if the audit ctx was canceled.
	defer func() {
		if err := shard.store.ResumeCompaction(context.Background()); err != nil {
			logger.WithField("shard", orphans[0].shardName).
				Warnf("reindex orphan audit: failed to resume compaction: %v", err)
		}
	}()

	cleaned := 0
	var failed []string
	for i := range orphans {
		o := &orphans[i]
		release, sealed := db.sealOrphanUnit(o)
		if !sealed {
			logger.WithField("orphan", o.String()).
				Warn("reindex orphan audit: a local unit of this migration is still running; leaving its tracker for the next audit")
			failed = append(failed, o.dirName)
			continue
		}
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task; quarantining sidecar bucket and tracker dir")
		err := func() error {
			// Deferred rather than called after: a seal that leaks refuses
			// this unit for the life of the process, so the migration could
			// never run here again.
			defer release()
			return db.cleanupOrphanTrackerCompactionPaused(ctx, shard, o, logger)
		}()
		if err != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: cleanup failed for tracker: %v", err)
			failed = append(failed, o.dirName)
			continue
		}
		cleaned++
	}
	return cleaned, failed
}

// cleanUnloadedShardOrphans removes orphan tracker dirs and their
// matching sidecar bucket dirs directly from disk. Used when the shard
// has not been loaded into the live DB; no in-memory bucket pointers
// or GlobalBucketRegistry entries exist for the orphan.
//
// Returns (cleanedCount, failedDirs) so the audit driver can roll up
// per-shard results into the typed [AuditOutcome] (S4).
func (db *DB) cleanUnloadedShardOrphans(lsmPath string, orphans []orphanReindexTracker, logger logrus.FieldLogger) (int, []string) {
	cleaned := 0
	var failed []string
	for i := range orphans {
		o := &orphans[i]
		release, sealed := db.sealOrphanUnit(o)
		if !sealed {
			logger.WithField("orphan", o.String()).
				Warn("reindex orphan audit: a local unit of this migration is still running; leaving its tracker for the next audit")
			failed = append(failed, o.dirName)
			continue
		}
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task on unloaded shard; removing tracker and sidecar dirs from disk")
		trackerPath := filepath.Join(lsmPath, ".migrations", o.dirName)
		removeErr := func() error {
			// Deferred rather than called after, for the reason above.
			defer release()
			if err := os.RemoveAll(trackerPath); err != nil {
				return err
			}
			removeUnloadedSidecarsForOrphan(lsmPath, o, logger)
			return nil
		}()
		if removeErr != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: failed to remove orphan tracker dir: %v", removeErr)
			failed = append(failed, o.dirName)
			continue
		}
		cleaned++
	}
	return cleaned, failed
}

// sealOrphanUnit holds the orphan's own (task, unit) for the length of its
// cleanup. The audit classifies an orphan from the task's cluster status
// alone, and a status goes terminal without waiting for the local unit to
// exit — so the tracker about to be deleted can be one a worker on this node
// is still writing through pointers taken before its phase began.
//
// A tracker with no payload.mig names no task, and the seal it takes is the
// empty descriptor, which holds nothing back. That is sound only because such
// a tracker cannot belong to a run this process started: the payload is
// written before any unit begins, and the caller has already excluded every
// directory created since process start.
func (db *DB) sealOrphanUnit(o *orphanReindexTracker) (func(), bool) {
	return db.migrationSeals.SealUnit(
		distributedtask.TaskDescriptor{ID: o.taskID, Version: o.taskVersion}, o.unitID)
}

// removeUnloadedSidecarsForOrphan removes the per-property sidecar bucket
// directories the orphan tracker owns: <main>__<ingestSuffix>_<gen> and
// <main>__<reindexSuffix>_<gen>, composed through [migrationSuffixes] keyed by
// the tracker's own dir name rather than matched by string prefix. A dir name
// matching no registered strategy is a no-op, so a new strategy is picked up
// here automatically.
//
// The registry rather than the record, because an orphan from the record-less
// arm has only its payload. A record's own [MigrationSubject.SidecarDirs]
// would be stronger evidence and moving to it is open work; until then the two
// derivations must not drift apart.
func removeUnloadedSidecarsForOrphan(lsmPath string, o *orphanReindexTracker, logger logrus.FieldLogger) {
	for _, sidecar := range sidecarDirsForOrphan(o) {
		path := filepath.Join(lsmPath, sidecar)
		if !fileExists(path) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			logger.WithField("path", path).
				Warnf("reindex orphan audit: failed to remove orphan sidecar dir: %v", err)
		}
	}
}

// sidecarDirsForOrphan returns the lsm-relative sidecar bucket dir names the
// strategy registry says this orphan's tracker owns. Returns an empty slice
// when the tracker dirName matches no registered strategy, or when the orphan
// carries no properties — class-level cleanup removes the tracker dir itself.
func sidecarDirsForOrphan(o *orphanReindexTracker) []string {
	return migrationSidecarDirsFor(o.dirName, o.prefix, o.generation, o.properties)
}

// migrationSidecarDirsFor names the sidecar bucket dirs the reclaiming audit
// may remove for one tracker: <main><ingestSuffix>_<gen> and
// <main><reindexSuffix>_<gen>, composed through [migrationSuffixes] keyed by
// the tracker's own dir name rather than matched by string prefix. A new
// strategy is therefore picked up automatically.
func migrationSidecarDirsFor(dirName, prefix string, generation int, properties []string) []string {
	if len(properties) == 0 {
		return nil
	}
	suffixes := migrationSuffixes(dirName)
	if suffixes == nil {
		return nil
	}
	reindexSuffix := reindexSuffixFor(prefix)
	genTail := genSuffix(generation)
	out := make([]string, 0, 3*len(properties))
	for _, propName := range properties {
		main := suffixes.sourceBucketName(propName)
		out = append(out, main+suffixes.ingestSuffix+genTail)
		if reindexSuffix != "" {
			out = append(out, main+reindexSuffix+genTail)
		}
	}
	return out
}

// orphanCleanupPauseTimeout bounds how long the audit waits for an
// in-flight compaction to drain before deferring cleanup of one
// tracker to the next process restart.
const orphanCleanupPauseTimeout = 5 * time.Minute

// cleanupOrphanTrackerCompactionPaused invokes
// CleanStalePartialReindexState for every (property, indexType) the
// orphan claims. The caller must hold [Store.PauseCompaction] for the
// duration of every orphan cleanup on the shard.
func (db *DB) cleanupOrphanTrackerCompactionPaused(ctx context.Context, shard *Shard, o *orphanReindexTracker, logger logrus.FieldLogger) error {
	if len(o.properties) == 0 || len(o.indexTypes) == 0 {
		// Class-level migration with no per-property indexType: fall back
		// to direct tracker-dir removal to reclaim disk space.
		trackerPath := filepath.Join(shard.pathLSM(), ".migrations", o.dirName)
		if err := os.RemoveAll(trackerPath); err != nil {
			return fmt.Errorf("remove orphan tracker dir %q: %w", trackerPath, err)
		}
		logger.WithField("orphan", o.String()).
			Info("reindex orphan audit: removed class-level tracker dir")
		return nil
	}

	for _, propName := range o.properties {
		for _, indexType := range o.indexTypes {
			// The audit reports per orphan, not per payload, so the read count
			// has no line here to land on.
			if _, err := shard.CleanStalePartialReindexState(ctx, propName, indexType); err != nil {
				return fmt.Errorf("clean stale partial reindex state for (prop=%q,indexType=%q): %w", propName, indexType, err)
			}
		}
	}
	return nil
}

// semanticMigrationIndexTypesForAudit returns the indexType fan-out
// the audit's CleanStalePartialReindexState loop iterates over for a
// given migration type. Mirrors [indexTypesFromMigrationType] in the
// REST handler.
//
// An empty type is a tracker from a release that recorded none, and the
// audit removes its directory whole. A type this build does not know is the
// opposite — a fan-out that exists but that nothing here can compose — so the
// second result is false and the caller reclaims nothing rather than
// deleting on a list it could not read.
func semanticMigrationIndexTypesForAudit(mt ReindexMigrationType) (indexTypes []string, known bool) {
	switch mt {
	case "":
		return nil, true
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}, true
	case ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}, true
	case ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable:
		return []string{"searchable"}, true
	case ReindexTypeEnableFilterable, ReindexTypeRepairFilterable:
		return []string{"filterable"}, true
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return []string{"rangeable"}, true
	}
	return nil, false
}
