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

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/schema"
)

// RecoveredReindex describes one in-flight reindex task discovered on
// disk at startup, together with the [ShardReindexTaskGeneric] instances
// reconstructed from the persisted payload. There is one
// RecoveredReindex per (TaskDescriptor, unitID, shard) — i.e. per
// migration directory observed on disk. For semantic migrations
// (change-tokenization) there are two task instances per unit (one
// searchable, one filterable); they share the same TaskDescriptor and
// UnitID.
//
// Callers use these to:
//
//  1. Hand the Tasks to the DB via [DB.SetRecoveredReindexTasks] before
//     [DB.WaitForStartup] runs, so the [OnAfterLsmInit] hook fires
//     during shard load and re-installs the double-write callbacks
//     BEFORE any post-restart write can reach the shard. Without this,
//     writes that arrive between shard init and the swap that completes
//     a deferred reindex go only to the old main bucket and are lost
//     when the swap replaces it with the ingest bucket.
//
//  2. Pre-populate [ReindexProvider.reindexTasks] so that
//     [OnGroupCompleted]'s swap phase reuses these same instances rather
//     than creating fresh ones and re-running [OnAfterLsmInit] (which
//     would attempt to load already-loaded ingest buckets).
type RecoveredReindex struct {
	Descriptor distributedtask.TaskDescriptor
	UnitID     string
	Collection string
	ShardName  string
	Tasks      []*ShardReindexTaskGeneric
}

// DiscoverInFlightReindexTasks walks every shard's
// .migrations/<migrationDir>/ at startup and reconstructs
// [ShardReindexTaskGeneric] instances for the recovery window where the
// reindex iteration is terminal but the swap has not yet completed.
//
// Reads payload.mig (the typed task payload persisted by
// persistRecoveryRecord before the iteration ran) and consults the
// sentinel files: started.mig (iteration started), reindexed.mig
// (iteration terminal), merged.mig (PREP complete), swapped.mig
// (in-memory swap complete), tidied.mig (swap fully tidied; no
// recovery needed).
//
// Returns a flat slice; deduplication across sibling migration dirs
// belonging to the same task is the caller's job.
func DiscoverInFlightReindexTasks(
	rootPath string,
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) ([]RecoveredReindex, error) {
	if rootPath == "" {
		return nil, nil
	}
	indices, err := os.ReadDir(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read root %q: %w", rootPath, err)
	}

	var recovered []RecoveredReindex
	for _, indexEntry := range indices {
		if !indexEntry.IsDir() {
			continue
		}
		indexPath := filepath.Join(rootPath, indexEntry.Name())
		shards, err := os.ReadDir(indexPath)
		if err != nil {
			continue
		}
		for _, shardEntry := range shards {
			if !shardEntry.IsDir() {
				continue
			}
			shardName := shardEntry.Name()
			lsmPath := filepath.Join(indexPath, shardName, "lsm")
			migrationsDir := filepath.Join(lsmPath, ".migrations")
			migs, err := os.ReadDir(migrationsDir)
			if err != nil {
				// Most shards have no .migrations dir; that's the normal path.
				continue
			}
			for _, migEntry := range migs {
				if !migEntry.IsDir() {
					continue
				}
				migDir := filepath.Join(migrationsDir, migEntry.Name())
				rec, ok := loadReindexRecoveryRecord(migDir, logger)
				if !ok {
					continue
				}

				// Parse the per-node generation suffix from the migration dir
				// name. With per-migration generation, the strategy instances
				// reconstructed here MUST use the same gen as the in-flight
				// state on disk, otherwise their SourceBucketName / Reindex
				// SuffixName paths won't match the on-disk dirs.
				_, generation, parseOk := parseMigrationDirName(migEntry.Name())
				if !parseOk {
					logger.WithField("migrationDir", migDir).
						Warn("reindex recovery: migration dir name missing _<gen> suffix; skipping")
					continue
				}

				tasks, err := buildRecoveryTasks(rec, shardName, generation, logger, schemaManager)
				if err != nil {
					logger.WithField("migrationDir", migDir).
						Warnf("reindex recovery: skipping migration; cannot build tasks: %v", err)
					continue
				}
				recovered = append(recovered, RecoveredReindex{
					Descriptor: distributedtask.TaskDescriptor{
						ID:      rec.TaskID,
						Version: rec.TaskVersion,
					},
					UnitID:     rec.UnitID,
					Collection: rec.Payload.Collection,
					ShardName:  shardName,
					Tasks:      tasks,
				})
			}
		}
	}
	return recovered, nil
}

// loadReindexRecoveryRecord reads payload.mig from a migration directory
// and returns the decoded record. Returns ok=false if:
//   - payload.mig is missing (older migration without the recovery
//     record, or no migration in progress);
//   - started.mig is missing (nothing has happened yet, no callbacks to
//     restore);
//   - reindexed.mig is missing (the reindex iteration is not yet
//     terminal — the DTM scheduler will call StartTask post-restart,
//     which re-registers callbacks via OnAfterLsmInit on a fresh task
//     instance; if we ALSO registered one here we'd end up with
//     duplicate double-write callbacks);
//   - tidied.mig is present (the migration is fully done — leftover
//     state will be cleaned up by [FinalizeCompletedMigrations]).
//
// The reindexed-but-not-tidied window is exactly the bug fixed by this
// recovery path: the unit is terminal in RAFT (so the scheduler will
// NOT call StartTask post-restart) but the swap (driven by
// OnGroupCompleted on the next scheduler tick) has not yet happened.
// Any write that arrives between shard init and that tick must land in
// the ingest bucket via a double-write callback, and the only way to
// have those callbacks active that early is to re-register them during
// shard init from on-disk state.
func loadReindexRecoveryRecord(migDir string, logger logrus.FieldLogger) (reindexRecoveryRecord, bool) {
	var rec reindexRecoveryRecord
	if !fileExists(filepath.Join(migDir, "started.mig")) {
		return rec, false
	}
	if !fileExists(filepath.Join(migDir, "reindexed.mig")) {
		return rec, false
	}
	if fileExists(filepath.Join(migDir, "tidied.mig")) {
		return rec, false
	}
	payloadPath := filepath.Join(migDir, reindexRecoveryPayloadFile)
	data, err := os.ReadFile(payloadPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.WithField("path", payloadPath).
				Warnf("reindex recovery: failed to read payload.mig: %v", err)
		}
		return rec, false
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		logger.WithField("path", payloadPath).
			Warnf("reindex recovery: malformed payload.mig; skipping: %v", err)
		return rec, false
	}
	return rec, true
}

// buildRecoveryTasks reconstructs the [ShardReindexTaskGeneric]
// instances that processOneUnit would have created for this migration
// type, but scoped to exactly the named shard. The scope is what makes
// per-instance callbackDisableFuncs safe to share with [runtimeSwap]
// later: the static reindexer iterates all registered tasks on every
// shard init, but each task's isShardSelected filter drops everything
// except the one shard the record came from.
func buildRecoveryTasks(
	rec reindexRecoveryRecord,
	shardName string,
	generation int,
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) ([]*ShardReindexTaskGeneric, error) {
	payload := rec.Payload
	if payload.Collection == "" {
		return nil, fmt.Errorf("payload missing collection")
	}
	var raw []*ShardReindexTaskGeneric
	switch payload.MigrationType {
	case ReindexTypeChangeAlgorithm:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(logger, schemaManager, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeRepairFilterable:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeRoaringSetRefreshTask(logger, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeFilterableToRangeableTask(logger, schemaManager, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeEnableFilterable:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeEnableFilterableTask(logger, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeEnableSearchable:
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("%s requires targetTokenization", payload.MigrationType)
		}
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeEnableSearchableTask(logger, payload.Properties, payload.Collection, payload.TargetTokenization, generation),
		}
	case ReindexTypeChangeTokenization:
		if len(payload.Properties) != 1 {
			return nil, fmt.Errorf("change-tokenization requires exactly one property")
		}
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("change-tokenization requires targetTokenization")
		}
		if payload.BucketStrategy == "" {
			return nil, fmt.Errorf("change-tokenization requires bucketStrategy")
		}
		propName := payload.Properties[0]
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeSearchableRetokenizeTask(
				logger, propName, payload.TargetTokenization,
				payload.Collection, payload.BucketStrategy, payload.Collection,
				generation,
			),
			NewRuntimeFilterableRetokenizeTask(
				logger,
				propName, payload.TargetTokenization,
				payload.Collection, payload.Collection,
				generation,
			),
		}
	case ReindexTypeChangeTokenizationFilterable:
		if len(payload.Properties) != 1 {
			return nil, fmt.Errorf("change-tokenization-filterable requires exactly one property")
		}
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("change-tokenization-filterable requires targetTokenization")
		}
		propName := payload.Properties[0]
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeFilterableRetokenizeTask(
				logger,
				propName, payload.TargetTokenization,
				payload.Collection, payload.Collection,
				generation,
			),
		}
	default:
		return nil, fmt.Errorf("unknown migration type %q", payload.MigrationType)
	}

	// Constrain each task to exactly this shard so multiple recovered
	// instances (one per shard) don't fight over the same
	// callbackDisableFuncs slice when [runtimeSwap] runs per-shard.
	for _, t := range raw {
		t.constrainToShard(payload.Collection, shardName)
	}
	return raw, nil
}

// SeedReindexProviderFromRecovery pre-populates the provider's
// per-descriptor task cache with instances reconstructed during startup
// recovery. The purpose is to make [ReindexProvider.OnGroupCompleted]
// reuse the recovered instances — whose double-write callbacks were
// re-registered during shard init — rather than fall through to the
// rehydrate branch and call [OnAfterLsmInit] a second time (which would
// attempt to load already-loaded ingest buckets).
//
// Pass the same slice as was given to [DB.SetRecoveredReindexTasks]
// so the in-memory instances stay in sync between the two consumers.
func SeedReindexProviderFromRecovery(provider *ReindexProvider, recovered []RecoveredReindex) {
	if provider == nil || len(recovered) == 0 {
		return
	}
	perDescUnit := map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{}
	for _, rr := range recovered {
		if rr.UnitID == "" {
			continue
		}
		if perDescUnit[rr.Descriptor] == nil {
			perDescUnit[rr.Descriptor] = map[string][]*ShardReindexTaskGeneric{}
		}
		perDescUnit[rr.Descriptor][rr.UnitID] = append(
			perDescUnit[rr.Descriptor][rr.UnitID], rr.Tasks...)
	}
	provider.SeedReindexTaskCache(perDescUnit)
}

// constrainToShard narrows a task's shard selection to exactly the
// named shard of the named collection. After recovery this is called
// on every reconstructed task so the per-shard runtimeSwap / disable
// flow doesn't accidentally touch other shards' callbacks.
func (t *ShardReindexTaskGeneric) constrainToShard(collection, shardName string) {
	t.config.selectionEnabled = true
	if t.config.selectedShardsByCollection == nil {
		t.config.selectedShardsByCollection = map[string]map[string]struct{}{}
	}
	t.config.selectedShardsByCollection[collection] = map[string]struct{}{
		shardName: {},
	}
	// Give each per-shard task a unique name so log lines and error
	// messages stay distinguishable when several shards of the same
	// collection are recovered.
	if !strings.Contains(t.name, "[recovery:") {
		t.name = fmt.Sprintf("%s[recovery:%s/%s]", t.name, collection, shardName)
		t.logger = t.logger.WithField("task", t.name)
	}
}

// runRecoveredReindexTasks fires OnAfterLsmInit for every reindex task
// that startup recovery reconstructed from disk, re-installing their
// double-write callbacks before any post-restart write can reach the
// shard. A task that fails is logged and skipped: the shard must still
// come up.
func (s *Shard) runRecoveredReindexTasks(ctx context.Context) {
	for _, t := range s.recoveredReindexTasks {
		if err := t.OnAfterLsmInit(ctx, s); err != nil {
			s.index.logger.WithField("task", t.Name()).WithField("shard", s.Name()).
				Errorf("reindex recovery: after-LSM-init failed: %v", err)
		}
	}
}
