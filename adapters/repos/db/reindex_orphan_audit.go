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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

// KnownReindexTaskLookup reports whether (taskID, taskVersion) is live
// in the DTM scheduler snapshot. One instance is built per audit
// invocation so all per-tracker classifications share a consistent
// snapshot.
type KnownReindexTaskLookup func(taskID string, taskVersion uint64) bool

// KnownReindexTaskLookupBuilder returns a fresh [KnownReindexTaskLookup]
// for one audit invocation.
type KnownReindexTaskLookupBuilder func() KnownReindexTaskLookup

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
	Status        AuditOutcomeStatus
	ScannedCount  int
	OrphansFound  int
	OrphansClean  int
	FailedDirs    []string
	SkipReason    string
}

// SetReindexAuditDeps installs the builder and logger used by
// [DB.AuditOrphanReindexTrackersIfReady]. If
// [DB.AuditOrphanReindexTrackersIfReady] was invoked one or more times
// before this call (race between RAFT replay firing per-class-dir
// restores and the Scheduler.Start goroutine that installs deps), the
// counter is non-zero and a single replay sweep runs synchronously so
// the deferred audit work is not silently lost. Closes B2.
//
// Callers must therefore invoke SetReindexAuditDeps with the audit
// context they want any replay sweep to inherit; a closed context will
// cause the replay sweep to early-return on PauseCompaction.
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
	if _, err := db.AuditOrphanReindexTrackers(context.Background(), builder(), logger); err != nil {
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
	return db.AuditOrphanReindexTrackers(ctx, builder(), logger)
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
	if knownTask == nil {
		// A nil lookup would misclassify every in-flight migration as an
		// orphan. Refuse rather than auto-quarantine on a normal restart.
		logger.Error("reindex orphan audit: KnownReindexTaskLookup is nil; skipping audit")
		return AuditOutcome{Status: AuditStatusSkipped, SkipReason: "nil_lookup"},
			fmt.Errorf("reindex orphan audit: KnownReindexTaskLookup is nil")
	}

	auditLogger := logger.WithField("action", "reindex_orphan_audit")

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
	db.indexLock.RLock()
	loadedByID := make(map[string]*Index, len(db.indices))
	for id, idx := range db.indices {
		loadedByID[id] = idx
	}
	db.indexLock.RUnlock()

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
					continue
				}
				outcome.OrphansFound += len(orphans)
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
					cleaned, failed = db.cleanLoadedShardOrphans(ctx, shard, orphans, auditLogger)
				} else {
					cleaned, failed = cleanUnloadedShardOrphans(lsmPath, orphans, auditLogger)
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

// processStartTime captures the moment of process startup so the
// audit can distinguish tracker dirs whose payload.mig was simply not
// flushed yet (post-process-start, in-flight) from pre-PR cluster
// state (pre-process-start, no payload.mig will ever land because the
// writer side did not exist on the source cluster). Initialised once
// at package init: the audit may run multiple times, but the
// distinction is "was this tracker on disk before this process
// existed?", which only the first-boot timestamp can answer.
//
// Subtle: a tracker dir created shortly before process start but read
// shortly after will be classified as legacy. That is correct: the
// only writer to .migrations/ on this process is ReindexProvider,
// which writes payload.mig BEFORE creating started.mig. Any tracker
// dir on disk at process start without payload.mig is therefore not
// one this process wrote and must come from either a pre-PR cluster
// version, a backup-restored class dir from such a cluster, or a
// crash that interleaved between the not-yet-existent payload.mig
// write and an earlier-version started.mig write (impossible on this
// branch).
var processStartTime = time.Now()

// collectOrphanTrackers walks <lsmPath>/.migrations/ and returns every
// tracker dir classified as an orphan (started.mig present,
// tidied.mig/merged.mig absent, payload.mig parseable, and the
// referenced task not known to DTM). Read-only; cleanup is the
// caller's job.
//
// M8: a tracker dir with started.mig but NO payload.mig is the
// pre-PR-cluster shape. We treat it as a class-level orphan iff the
// tracker dir's mtime is at or before processStartTime — meaning the
// dir predates this process and is not in-flight. The orphan has no
// properties/indexTypes (the missing payload.mig is exactly the loss
// of that information) so the cleanup path falls back to direct
// tracker-dir removal. Newer trackers without payload.mig (mtime
// after process start) are skipped with a WARN and left for the next
// audit invocation — they may still be mid-flush.
func collectOrphanTrackers(lsmPath, collection, shardName string, knownTask KnownReindexTaskLookup, logger logrus.FieldLogger) []orphanReindexTracker {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
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
		if fileExistsInDir(trackerPath, "tidied.mig") || fileExistsInDir(trackerPath, "merged.mig") {
			continue
		}
		if !fileExistsInDir(trackerPath, "started.mig") {
			continue
		}
		rec, recOK := loadAuditRecord(trackerPath)
		if !recOK {
			// M8: distinguish pre-PR legacy state (no payload.mig will
			// ever land — the writer did not exist on the source
			// cluster) from a tracker we're racing the writer on.
			legacy, mtime, classifyErr := isLegacyTrackerWithoutPayload(trackerPath)
			if classifyErr != nil {
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).
					Warnf("reindex orphan audit: tracker missing payload.mig and mtime unreadable; manual cleanup may be needed: %v", classifyErr)
				continue
			}
			if !legacy {
				// Tracker dir created post-process-start: a write
				// could still be in flight. Leave for next audit.
				logger.WithField("collection", collection).WithField("shard", shardName).
					WithField("tracker", dirName).WithField("tracker_mtime", mtime).
					Warn("reindex orphan audit: tracker missing payload.mig but mtime is post-process-start; " +
						"leaving for next audit invocation (writer race or upstream bug)")
				continue
			}
			// Pre-PR cluster orphan: enqueue as class-level cleanup.
			// We cannot recover taskID / properties / indexTypes
			// (they were never persisted), so the loaded-shard
			// fallback in cleanupOrphanTrackerCompactionPaused
			// removes the tracker dir directly to reclaim disk.
			logger.WithField("collection", collection).WithField("shard", shardName).
				WithField("tracker", dirName).WithField("tracker_mtime", mtime).
				Warn("reindex orphan audit: pre-PR-cluster tracker dir without payload.mig (mtime <= process start); quarantining as class-level orphan")
			orphans = append(orphans, orphanReindexTracker{
				collection: collection,
				shardName:  shardName,
				dirName:    dirName,
				prefix:     prefix,
				generation: generation,
				// Empty taskID / properties / indexTypes drives the
				// class-level cleanup branch in
				// cleanupOrphanTrackerCompactionPaused.
			})
			continue
		}
		if knownTask(rec.TaskID, rec.TaskVersion) {
			continue
		}
		orphans = append(orphans, orphanReindexTracker{
			collection:  collection,
			shardName:   shardName,
			dirName:     dirName,
			prefix:      prefix,
			generation:  generation,
			taskID:      rec.TaskID,
			taskVersion: rec.TaskVersion,
			unitID:      rec.UnitID,
			properties:  append([]string(nil), rec.Payload.Properties...),
			indexTypes:  semanticMigrationIndexTypesForAudit(rec.Payload.MigrationType),
		})
	}
	return orphans
}

// isLegacyTrackerWithoutPayload returns true if the tracker dir was
// last modified at or before this process started, signalling
// pre-PR-cluster state with no payload.mig. Returns the mtime so
// callers can include it in logs for forensics.
func isLegacyTrackerWithoutPayload(trackerPath string) (bool, time.Time, error) {
	info, err := os.Stat(trackerPath)
	if err != nil {
		return false, time.Time{}, err
	}
	mtime := info.ModTime()
	return !mtime.After(processStartTime), mtime, nil
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
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task; quarantining sidecar bucket and tracker dir")
		if err := db.cleanupOrphanTrackerCompactionPaused(ctx, shard, o, logger); err != nil {
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
func cleanUnloadedShardOrphans(lsmPath string, orphans []orphanReindexTracker, logger logrus.FieldLogger) (int, []string) {
	cleaned := 0
	var failed []string
	for i := range orphans {
		o := &orphans[i]
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task on unloaded shard; removing tracker and sidecar dirs from disk")
		trackerPath := filepath.Join(lsmPath, ".migrations", o.dirName)
		if err := os.RemoveAll(trackerPath); err != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: failed to remove orphan tracker dir: %v", err)
			failed = append(failed, o.dirName)
			continue
		}
		removeUnloadedSidecarsForOrphan(lsmPath, o, logger)
		cleaned++
	}
	return cleaned, failed
}

// removeUnloadedSidecarsForOrphan removes per-property sidecar bucket
// directories owned by the orphan tracker. Routes through the strategy
// registry (migrationSuffixes) keyed by the orphan's tracker dirName
// — the strategy's MigrationDirName() and IngestSuffix/BackupSuffix/
// ReindexSuffix methods are the single source of truth for the on-disk
// dir layout (S3 fix). Falls back to no-op if the tracker dirName does
// not match any registered strategy: defensive, but it also means a
// future strategy added to migrationSuffixes will be picked up here
// automatically.
//
// Sidecar dir names that this consults:
//   - <main>__<ingestSuffix>_<gen>      (ingest sidecar)
//   - <main>__<backupSuffix>_<gen>      (backup sidecar)
//   - <main>__<reindexSuffix>_<gen>     (reindex sidecar)
//
// where `<main>` is the strategy's sourceBucketName(propName) for the
// canonical bucket the migration writes back to. The strategy decides
// what those names are — the audit MUST NOT re-derive them by string
// prefix.
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

// sidecarDirsForOrphan returns the lsm-relative sidecar bucket dir
// names the strategy registry says are owned by this orphan's tracker
// dir + property set + generation. Computed by consulting
// [migrationSuffixes] keyed off the orphan's tracker dirName: the
// strategy itself owns the IngestSuffix / BackupSuffix /
// ReindexSuffix tail base, and the audit appends the matching
// `_<gen>` to each. Returns an empty slice when the tracker dirName
// does not match any registered strategy or when the orphan carries
// no properties (class-level cleanup is handled by the caller via
// direct tracker-dir removal).
//
// Closes S3 by routing through the strategy registry instead of
// re-deriving sidecar names by hard-coded string prefix.
func sidecarDirsForOrphan(o *orphanReindexTracker) []string {
	if len(o.properties) == 0 {
		return nil
	}
	suffixes := migrationSuffixes(o.dirName)
	if suffixes == nil {
		return nil
	}
	reindexSuffix := reindexSuffixForFinalize(o.prefix)
	genTail := genSuffix(o.generation)
	out := make([]string, 0, 3*len(o.properties))
	for _, propName := range o.properties {
		main := suffixes.sourceBucketName(propName)
		out = append(out,
			main+suffixes.ingestSuffix+genTail,
			main+suffixes.backupSuffix+genTail,
		)
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
			if err := shard.CleanStalePartialReindexState(ctx, propName, indexType); err != nil {
				return fmt.Errorf("clean stale partial reindex state for (prop=%q,indexType=%q): %w", propName, indexType, err)
			}
		}
	}
	return nil
}

// loadAuditRecord reads the payload.mig recovery record for a tracker
// dir. Returns false if missing or unparseable. Sentinel-file presence
// checks are the caller's responsibility.
func loadAuditRecord(trackerPath string) (reindexRecoveryRecord, bool) {
	var rec reindexRecoveryRecord
	data, err := os.ReadFile(filepath.Join(trackerPath, reindexRecoveryPayloadFile))
	if err != nil {
		return rec, false
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return rec, false
	}
	return rec, true
}

// semanticMigrationIndexTypesForAudit returns the indexType fan-out
// the audit's CleanStalePartialReindexState loop iterates over for a
// given migration type. Mirrors [indexTypesFromMigrationType] in the
// REST handler. Returns nil for class-level migrations; the audit then
// falls back to direct tracker-dir removal.
func semanticMigrationIndexTypesForAudit(mt ReindexMigrationType) []string {
	switch mt {
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}
	case ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable:
		return []string{"searchable"}
	case ReindexTypeEnableFilterable, ReindexTypeRepairFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return []string{"rangeable"}
	}
	return nil
}
