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
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

// reindex.KnownReindexTaskLookup reports whether (taskID, taskVersion) is live
// in the DTM scheduler snapshot. One instance is built per audit
// invocation so all per-tracker classifications share a consistent
// snapshot.
// reindex.KnownReindexTaskLookupBuilder returns a fresh [reindex.KnownReindexTaskLookup]
// for one audit invocation.
// SetReindexAuditDeps installs the builder and logger used by
// [DB.AuditOrphanReindexTrackersIfReady].
func (db *DB) SetReindexAuditDeps(builder reindex.KnownReindexTaskLookupBuilder, logger logrus.FieldLogger) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexAuditLookupBuilder = builder
	db.reindexAuditLogger = logger
}

// AuditOrphanReindexTrackersIfReady is the no-arg wrapper for callers
// without the lookup builder in scope (e.g. the post-restore
// RestoreClassDir hook). Returns nil when deps are not yet installed;
// the startup audit will sweep later.
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
func (db *DB) AuditOrphanReindexTrackers(ctx context.Context, knownTask reindex.KnownReindexTaskLookup, logger logrus.FieldLogger) error {
	if logger == nil {
		logger = logrus.New()
	}
	if knownTask == nil {
		// A nil lookup would misclassify every in-flight migration as an
		// orphan. Refuse rather than auto-quarantine on a normal restart.
		logger.Error("reindex orphan audit: reindex.KnownReindexTaskLookup is nil; skipping audit")
		return fmt.Errorf("reindex orphan audit: reindex.KnownReindexTaskLookup is nil")
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

	// Snapshot loaded indexes so per-shard cleanup can route through
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
				orphans := collectOrphanTrackers(lsmPath, collection, shardName, knownTask, auditLogger)
				if len(orphans) == 0 {
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
			Warn("reindex orphan audit: cleanup complete")
	} else {
		auditLogger.Debug("reindex orphan audit: no orphan trackers found")
	}
	return nil
}

// collectOrphanTrackers walks <lsmPath>/.migrations/ and returns every
// tracker dir classified as an orphan (started.mig present,
// tidied.mig/merged.mig absent, payload.mig parseable, and the
// referenced task not known to DTM). Read-only; cleanup is the
// caller's job.
func collectOrphanTrackers(lsmPath, collection, shardName string, knownTask reindex.KnownReindexTaskLookup, logger logrus.FieldLogger) []orphanReindexTracker {
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
		prefix, generation, ok := reindex.ParseMigrationDirName(dirName)
		if !ok {
			continue
		}
		trackerPath := filepath.Join(migsDir, dirName)
		if reindex.FileExistsInDir(trackerPath, "tidied.mig") || reindex.FileExistsInDir(trackerPath, "merged.mig") {
			continue
		}
		if !reindex.FileExistsInDir(trackerPath, "started.mig") {
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
// single PauseCompaction window. A per-orphan pause/resume cycle would
// race the cycle manager: the resume between orphans lets a fresh
// compaction start on the next sidecar bucket, and the next pause
// times out trying to drain it.
func (db *DB) cleanLoadedShardOrphans(ctx context.Context, shard *Shard, orphans []orphanReindexTracker, logger logrus.FieldLogger) int {
	if len(orphans) == 0 {
		return 0
	}
	pauseCtx, cancelPause := context.WithTimeout(ctx, orphanCleanupPauseTimeout)
	defer cancelPause()
	if err := shard.Store().PauseCompaction(pauseCtx); err != nil {
		logger.WithField("collection", orphans[0].collection).WithField("shard", orphans[0].shardName).
			Warnf("reindex orphan audit: failed to pause compaction; skipping shard cleanup: %v", err)
		return 0
	}
	// Resume must fire even if the audit ctx was canceled.
	defer func() {
		if err := shard.Store().ResumeCompaction(context.Background()); err != nil {
			logger.WithField("shard", orphans[0].shardName).
				Warnf("reindex orphan audit: failed to resume compaction: %v", err)
		}
	}()

	cleaned := 0
	for i := range orphans {
		o := &orphans[i]
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task; quarantining sidecar bucket and tracker dir")
		if err := db.cleanupOrphanTrackerCompactionPaused(ctx, shard, o, logger); err != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: cleanup failed for tracker: %v", err)
			continue
		}
		cleaned++
	}
	return cleaned
}

// cleanUnloadedShardOrphans removes orphan tracker dirs and their
// matching sidecar bucket dirs directly from disk. Used when the shard
// has not been loaded into the live DB; no in-memory bucket pointers
// or GlobalBucketRegistry entries exist for the orphan.
func cleanUnloadedShardOrphans(lsmPath string, orphans []orphanReindexTracker, logger logrus.FieldLogger) int {
	cleaned := 0
	for i := range orphans {
		o := &orphans[i]
		logger.WithField("orphan", o.String()).
			Warn("reindex orphan audit: found tracker for unknown task on unloaded shard; removing tracker and sidecar dirs from disk")
		trackerPath := filepath.Join(lsmPath, ".migrations", o.dirName)
		if err := os.RemoveAll(trackerPath); err != nil {
			logger.WithField("orphan", o.String()).
				Warnf("reindex orphan audit: failed to remove orphan tracker dir: %v", err)
			continue
		}
		removeUnloadedSidecarsForOrphan(lsmPath, o, logger)
		cleaned++
	}
	return cleaned
}

// removeUnloadedSidecarsForOrphan removes per-property sidecar bucket
// directories that match the orphan's per-property prefix and
// generation. The strategy-specific suffix is unknown without the
// strategy instance, so it scans the lsm dir and matches by canonical
// property prefix and the _<N> generation suffix.
func removeUnloadedSidecarsForOrphan(lsmPath string, o *orphanReindexTracker, logger logrus.FieldLogger) {
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return
	}
	genSuffixStr := reindex.GenSuffixForDebug(o.generation)
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
					Warnf("reindex orphan audit: failed to remove orphan sidecar dir: %v", err)
			}
		}
	}
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
func loadAuditRecord(trackerPath string) (reindex.ReindexRecoveryRecord, bool) {
	var rec reindex.ReindexRecoveryRecord
	data, err := os.ReadFile(filepath.Join(trackerPath, reindex.ReindexRecoveryPayloadFile))
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
func semanticMigrationIndexTypesForAudit(mt reindex.ReindexMigrationType) []string {
	switch mt {
	case reindex.ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}
	case reindex.ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}
	case reindex.ReindexTypeEnableSearchable, reindex.ReindexTypeChangeAlgorithm, reindex.ReindexTypeRebuildSearchable:
		return []string{"searchable"}
	case reindex.ReindexTypeEnableFilterable, reindex.ReindexTypeRepairFilterable:
		return []string{"filterable"}
	case reindex.ReindexTypeEnableRangeable, reindex.ReindexTypeRepairRangeable:
		return []string{"rangeable"}
	}
	return nil
}
