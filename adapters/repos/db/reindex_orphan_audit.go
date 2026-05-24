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
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// KnownReindexTaskLookup reports whether (taskID, taskVersion) is
// "live" in the DTM scheduler snapshot. A KnownReindexTaskLookup
// instance is single-audit-invocation-scoped: built once at the start
// of an audit by [KnownReindexTaskLookupBuilder] so the audit's
// per-tracker classification consults one consistent snapshot
// instead of issuing a RAFT read per tracker (Copilot review).
type KnownReindexTaskLookup func(taskID string, taskVersion uint64) bool

// KnownReindexTaskLookupBuilder returns a fresh
// [KnownReindexTaskLookup] for one audit invocation. The audit calls
// it once at entry and reuses the returned lookup for the entire walk.
type KnownReindexTaskLookupBuilder func() KnownReindexTaskLookup

// SetReindexAuditDeps installs (builder, logger). Called once from
// the Scheduler-bootstrap goroutine. The mutex makes the
// (builder, logger) tuple atomically visible to concurrent readers.
func (db *DB) SetReindexAuditDeps(builder KnownReindexTaskLookupBuilder, logger logrus.FieldLogger) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexAuditLookupBuilder = builder
	db.reindexAuditLogger = logger
}

// AuditOrphanReindexTrackersIfReady is the no-arg wrapper used by
// callers that don't have the lookup builder in scope (canonical:
// the post-restore RestoreClassDir hook on a RAFT FSM apply goroutine).
// Returns nil when deps aren't set yet — startup audit will sweep
// any orphans once wiring completes.
func (db *DB) AuditOrphanReindexTrackersIfReady(ctx context.Context) error {
	db.reindexAuditMu.RLock()
	builder := db.reindexAuditLookupBuilder
	logger := db.reindexAuditLogger
	db.reindexAuditMu.RUnlock()
	if builder == nil {
		return nil
	}
	return db.AuditOrphanReindexTrackers(ctx, builder(), logger)
}

// orphanReindexTracker carries the fields the cleanup loop needs to
// thread into its per-orphan WARN log.
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

// String formats one keyed line per field — log queries can grep
// any field (taskID, dir, collection) without parsing.
func (o *orphanReindexTracker) String() string {
	return fmt.Sprintf(
		"collection=%q shard=%q tracker=%q gen=%d taskID=%q taskVersion=%d unitID=%q properties=%v indexTypes=%v",
		o.collection, o.shardName, o.dirName, o.generation,
		o.taskID, o.taskVersion, o.unitID, o.properties, o.indexTypes)
}

// AuditOrphanReindexTrackers quarantines `.migrations/<tracker>/`
// dirs whose payload.mig references a (TaskID, TaskVersion) the DTM
// scheduler doesn't know about. Canonical case: restored cluster
// whose backup captured the tracker but not the DTM unit driving it
// (0-weaviate-issues#215 B3).
//
// Per orphan: structured WARN + [Shard.CleanStalePartialReindexState]
// per touched (property, indexType) — shuts down + removes the
// sidecar bucket, the dir, and the tracker. The canonical main
// bucket is never touched (it serves pre-migration data on a
// restored cluster because the schema flip never committed).
//
// Pre-conditions: DTM bootstrap done on this node, shards loaded.
// Cold lazy MT shards are skipped (re-evaluated at next activation).
// Best-effort: per-orphan errors logged, return value reserved for
// future composition.
func (db *DB) AuditOrphanReindexTrackers(ctx context.Context, knownTask KnownReindexTaskLookup, logger logrus.FieldLogger) error {
	if logger == nil {
		logger = logrus.New()
	}
	if knownTask == nil {
		// Defensive: a nil lookup means "every task is unknown", which
		// would auto-quarantine in-flight migrations during a normal
		// restart. Refuse loudly rather than wreck production.
		logger.Error("reindex orphan audit: KnownReindexTaskLookup is nil; skipping audit (every legitimate in-flight reindex would be misclassified as an orphan)")
		return fmt.Errorf("reindex orphan audit: KnownReindexTaskLookup is nil")
	}

	auditLogger := logger.WithField("action", "reindex_orphan_audit")

	rootPath := db.config.RootPath
	if rootPath == "" {
		return nil
	}

	indexEntries, err := os.ReadDir(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		auditLogger.WithField("path", rootPath).
			Warnf("reindex orphan audit: cannot read root path; skipping audit: %v", err)
		return nil
	}

	// indexID-dir → loaded Index map: routes per-shard cleanup through
	// in-memory state when the shard is loaded.
	db.indexLock.RLock()
	loadedByID := make(map[string]*Index, len(db.indices))
	for id, idx := range db.indices {
		loadedByID[id] = idx
	}
	db.indexLock.RUnlock()

	var orphanCount int
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
		// Hold idx.dropIndex.RLock while we read idx.Config / idx.shards
		// to prevent a concurrent Drop/DeleteClass from tearing the
		// Index down underneath the audit. See index.go:253-265 for
		// the same convention on every other Index user.
		processIndex := func() {
			// Loaded-index branch uses the real class name; unloaded
			// fallback uses the on-disk dir name (indexID transform is
			// irreversible without consulting the schema).
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
				orphans := collectOrphanTrackers(lsmPath, collection, shardName, knownTask, auditLogger)
				if len(orphans) == 0 {
					continue
				}
				// Loaded shard: route through in-memory bucket pointers +
				// GlobalBucketRegistry. Unloaded (post-restore-pre-load
				// or cold MT tenant): direct disk-only cleanup;
				// submit/cancel hooks handle stray state on activation.
				var shard *Shard
				if idx != nil {
					if sl := idx.shards.Load(shardName); sl != nil {
						if s, ok := sl.(*Shard); ok {
							shard = s
						}
					}
				}
				if shard != nil {
					orphanCount += db.cleanLoadedShardOrphans(ctx, shard, orphans, auditLogger)
				} else {
					orphanCount += cleanUnloadedShardOrphans(lsmPath, orphans, auditLogger)
				}
			}
		}
		processIndex()
	}

	if orphanCount > 0 {
		auditLogger.WithField("orphan_count", orphanCount).
			Warn("reindex orphan audit: cleanup complete; restored cluster reached self-consistent state (canonical buckets retained, orphan sidecars and tracker dirs removed)")
	} else {
		auditLogger.Debug("reindex orphan audit: no orphan trackers found")
	}
	return nil
}

// collectOrphanTrackers walks `<lsmPath>/.migrations/` and returns
// every tracker dir classified as an orphan (started.mig present,
// tidied.mig/merged.mig absent, payload.mig parseable, and the
// referenced (taskID, version) NOT known to DTM). The function does
// NOT modify any state — that's the caller's job, since the cleanup
// path depends on whether the shard is loaded.
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
			logger.WithField("collection", collection).WithField("shard", shardName).
				WithField("tracker", dirName).
				Warn("reindex orphan audit: tracker missing payload.mig; manual cleanup may be needed")
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

// cleanLoadedShardOrphans cleans every orphan on the shard under a
// single PauseCompaction window. Per-orphan pause/resume previously
// raced: the defer re-enabled compaction between orphans, a fresh
// compaction started on the next orphan's sidecar bucket, and the
// next pause timed out trying to drain it.
//
// The audit's pause path does NOT coordinate with backup's
// [Index.HaltForTransfer] via haltForTransferMux. A concurrent backup
// is gated upstream by [Backupable] / [Index.refuseIfReindexInFlight]
// — it refuses on the not-yet-removed tracker dirs before reaching
// Deactivate.
func (db *DB) cleanLoadedShardOrphans(ctx context.Context, shard *Shard, orphans []orphanReindexTracker, logger logrus.FieldLogger) int {
	if len(orphans) == 0 {
		return 0
	}
	pauseCtx, cancelPause := context.WithTimeout(ctx, orphanCleanupPauseTimeout)
	defer cancelPause()
	if err := shard.store.PauseCompaction(pauseCtx); err != nil {
		logger.WithField("collection", orphans[0].collection).WithField("shard", orphans[0].shardName).
			Warnf("reindex orphan audit: failed to pause compaction on shard; skipping all orphan cleanups on this shard (next restart will retry): %v", err)
		return 0
	}
	// Resume must fire even if the audit ctx was cancelled.
	defer func() {
		if err := shard.store.ResumeCompaction(context.Background()); err != nil {
			logger.WithField("shard", orphans[0].shardName).
				Warnf("reindex orphan audit: failed to resume compaction after orphan cleanup; the next restart will resume it naturally: %v", err)
		}
	}()

	cleaned := 0
	for i := range orphans {
		o := &orphans[i]
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task (typically backup-restore of a pre-#215-fix payload); quarantining sidecar bucket + tracker dir")
		if err := db.cleanupOrphanTrackerCompactionPaused(ctx, shard, o, logger); err != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: cleanup failed for tracker; manual intervention may be required to reclaim the disk space: %v", err)
			continue
		}
		cleaned++
	}
	return cleaned
}

// cleanUnloadedShardOrphans removes orphan tracker dirs + their
// matching sidecar bucket dirs directly from disk. Used when the
// shard has not been loaded into the live DB yet — the typical
// post-restore-before-FSM-apply window. No in-memory bucket pointers
// or GlobalBucketRegistry entries exist for the orphan, so plain
// `os.RemoveAll` is sufficient.
//
// On the post-restore path the FSM has not yet applied the schema and
// the *Shard struct does not exist — so there is no live state to
// disturb. Direct disk removal is the correct cleanup primitive
// here; when the FSM later loads the class for the first time, the
// shard init walks a clean `.migrations/` and `lsm/` directory.
func cleanUnloadedShardOrphans(lsmPath string, orphans []orphanReindexTracker, logger logrus.FieldLogger) int {
	cleaned := 0
	for i := range orphans {
		o := &orphans[i]
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task on unloaded shard (post-restore window); removing tracker dir + sidecar dirs from disk")
		trackerPath := filepath.Join(lsmPath, ".migrations", o.dirName)
		if err := os.RemoveAll(trackerPath); err != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: failed to remove orphan tracker dir; manual intervention may be required: %v", err)
			continue
		}
		removeUnloadedSidecarsForOrphan(lsmPath, o, logger)
		cleaned++
	}
	return cleaned
}

// removeUnloadedSidecarsForOrphan removes per-property sidecar bucket
// directories that match the orphan's per-property prefix and
// generation. Called only from the unloaded-shard cleanup path; no
// in-memory state is involved.
//
// We can't reproduce the exact strategy-specific suffix names without
// the strategy instance, so we scan the lsm dir and match by:
//
//   - canonical main-bucket prefix (e.g. `property_body__`,
//     `property_body_searchable__`, `property_body_rangeable__`); and
//   - the gen-suffix tail `_<N>` that the orphan tracker carries.
//
// This matches every sidecar variant the runtime-reindex code path
// produces (`__retokenize_reindex_<N>`, `__filt_retokenize_ingest_<N>`,
// `__blockmax_<N>`, etc.) without hard-coding the strategy suffix
// vocabulary.
func removeUnloadedSidecarsForOrphan(lsmPath string, o *orphanReindexTracker, logger logrus.FieldLogger) {
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return
	}
	genSuffixStr := genSuffix(o.generation)
	for _, propName := range o.properties {
		prefixes := []string{
			"property_" + propName + "__",
			"property_" + propName + "_searchable__",
			"property_" + propName + "_rangeable__",
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := entry.Name()
			matched := false
			for _, p := range prefixes {
				if strings.HasPrefix(name, p) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			if !strings.HasSuffix(name, genSuffixStr) {
				continue
			}
			path := filepath.Join(lsmPath, name)
			if err := os.RemoveAll(path); err != nil {
				logger.WithField("path", path).
					Warnf("reindex orphan audit: failed to remove orphan sidecar dir; manual intervention may be required: %v", err)
			}
		}
	}
}

// orphanCleanupPauseTimeout caps how long the audit waits for an
// in-flight compaction on the orphan sidecar bucket to drain before
// proceeding with cleanup. The default lsmkv compaction cycle target
// is in the low minutes for the segment sizes orphan sidecars usually
// carry; 5 minutes leaves headroom for a slow CI runner while still
// bounding worst case so a misbehaving compaction does not wedge the
// audit forever. On timeout the audit defers cleanup of that one
// tracker to the next process restart — that path picks up the same
// orphan because the tracker dir is still on disk, but with a
// different in-flight compaction state.
const orphanCleanupPauseTimeout = 5 * time.Minute

// cleanupOrphanTrackerCompactionPaused invokes
// CleanStalePartialReindexState for every (property, indexType) the
// orphan claims, which is the existing shutdown+remove+registry-clear
// path the cancel-handler uses.
//
// PRE-CONDITION: the caller (auditShardForOrphans) has already issued
// [Store.PauseCompaction] on this shard and holds the pause for the
// duration of every orphan cleanup on the shard. Pausing per-orphan
// would race the cycle manager: the resume in the defer between two
// orphans would allow a fresh compaction to start on a different
// orphan sidecar bucket, and the next pause would time out trying to
// drain it.
//
// Safe to call on a loaded shard concurrent with normal traffic: the
// inner function acquires the shard-local locks the rest of the
// runtime-reindex machinery already coordinates on, and the
// pause/resume primitives are the same ones [Shard.HaltForTransfer]
// uses on the backup path.
func (db *DB) cleanupOrphanTrackerCompactionPaused(ctx context.Context, shard *Shard, o *orphanReindexTracker, logger logrus.FieldLogger) error {
	if len(o.properties) == 0 || len(o.indexTypes) == 0 {
		// No (prop, indexType) pair to act on — likely a class-level
		// migration (Map→Blockmax) whose properties live inside the
		// strategy's per-property bookkeeping rather than the payload.
		// Fall back to a direct tracker-dir removal so disk usage is
		// reclaimed even when CleanStalePartialReindexState wouldn't
		// match anything.
		trackerPath := filepath.Join(shard.pathLSM(), ".migrations", o.dirName)
		if err := os.RemoveAll(trackerPath); err != nil {
			return fmt.Errorf("remove orphan tracker dir %q: %w", trackerPath, err)
		}
		logger.WithField("orphan", o.String()).
			Info("reindex orphan audit: removed class-level tracker dir (no property/indexType to clean via CleanStalePartialReindexState)")
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

// loadAuditRecord reads the on-disk recovery record for a tracker dir
// using the same payload.mig contract as
// [loadReindexRecoveryRecord], but with the looser sentinel-set gate
// the audit needs: payload.mig must be present and parseable; the
// started.mig / reindexed.mig / tidied.mig presence is the caller's
// responsibility (already filtered above).
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

// semanticMigrationIndexTypesForAudit returns the (property,
// indexType) fan-out the audit's CleanStalePartialReindexState loop
// iterates over. Mirrors the REST-layer's [indexTypesFromMigrationType]
// (handlers_indexes.go) so audit cleanup covers the same per-property
// classification as the cancel/cleanup dispatch.
//
// Truly class-level migrations (e.g. ChangeAlgorithm = Map→Blockmax,
// which carries no per-property indexType) fall through to the
// default and return nil; the audit then takes the direct
// tracker-dir removal branch in
// [DB.cleanupOrphanTrackerCompactionPaused].
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
	// Defensive: a future MigrationType added without a mapping here
	// must still be handled (direct removal is safe — orphan sidecar
	// dirs the canonical bucket doesn't reference cannot leak query
	// data). Class-level cleanup via os.RemoveAll on the tracker dir
	// reclaims the disk space; the per-property sidecars remain only
	// when a known prefix/indexType is added.
	return nil
}
