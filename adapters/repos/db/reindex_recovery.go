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
//  1. Register the Tasks with the static [ShardReindexerV3] before
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
// <root>/<index>/<shard>/lsm/.migrations/<migrationDir>/ directory at
// startup and reconstructs [ShardReindexTaskGeneric] instances for the
// narrow recovery window where the reindex iteration is terminal but
// the swap has not yet completed.
//
//   - The persisted recovery record (payload.mig) is written by
//     [ReindexProvider.persistRecoveryRecord] before the first reindex
//     iteration runs on a unit. It captures the typed task payload —
//     migration type, target tokenization, bucket strategy, properties,
//     UnitToShard mapping — plus the cluster-wide task descriptor. The
//     sentinel files consulted alongside it are:
//
//     started.mig    — set by the reindex iteration on first run
//     reindexed.mig  — set when iteration is terminal (per-shard); the
//     unit transitions to COMPLETED in RAFT around
//     the same time. If this is missing, the DTM
//     scheduler will call StartTask post-restart and
//     re-register callbacks itself, so recovery must
//     NOT register a second instance.
//     tidied.mig     — set after the runtime swap; the migration is
//     fully done and only directory renames remain
//     (handled by [FinalizeCompletedMigrations]).
//
//   - Per-shard task instances use selectedShardsByCollection scoped to
//     exactly the shard whose directory the record was found in. That
//     keeps callback registration / deregistration per-instance so the
//     callbackDisableFuncs slice in one shard's task instance can never
//     remove another shard's callback when [runtimeSwap] runs.
//
// Returns a flat slice; deduplication across migration directories that
// belong to the same task (e.g. searchable + filterable for one
// change-tokenization unit) is the caller's responsibility — usually
// they want both registered with the reindexer and grouped under the
// same (TaskDescriptor, UnitID) entry in ReindexProvider's cache.
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
					logger.WithField("migrationDir", migDir).WithError(err).
						Warn("reindex recovery: skipping migration; cannot build tasks")
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
			logger.WithField("path", payloadPath).WithError(err).
				Warn("reindex recovery: failed to read payload.mig")
		}
		return rec, false
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		logger.WithField("path", payloadPath).WithError(err).
			Warn("reindex recovery: malformed payload.mig; skipping")
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
	case ReindexTypeRepairSearchable:
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

// NewShardReindexerV3FromRecovered wires the recovered tasks into a
// fresh recovery-only [ShardReindexerV3]. The reindexer only fires
// [OnAfterLsmInit] — the iteration loop ([OnAfterLsmInitAsync]) is the
// DTM provider's job, and [OnBeforeLsmInit]'s restart-based merge/swap
// is intentionally skipped so the DTM's OnGroupCompleted is the single
// source of truth for the swap step. This keeps recovery's
// responsibility narrow: re-install the double-write callbacks before
// any writes arrive.
func NewShardReindexerV3FromRecovered(
	recovered []RecoveredReindex,
	logger logrus.FieldLogger,
) ShardReindexerV3 {
	r := newShardReindexerV3RecoveryOnly(logger)
	for _, rr := range recovered {
		for _, t := range rr.Tasks {
			r.registerTask(t)
		}
	}
	return r
}

// SeedReindexProviderFromRecovery pre-populates the provider's
// per-descriptor task cache with instances reconstructed during startup
// recovery. The purpose is to make [ReindexProvider.OnGroupCompleted]
// reuse the recovered instances — whose double-write callbacks were
// re-registered during shard init — rather than fall through to the
// rehydrate branch and call [OnAfterLsmInit] a second time (which would
// attempt to load already-loaded ingest buckets).
//
// Pass the same slice as was given to [NewShardReindexerV3FromRecovered]
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
	// Give each per-shard task a unique name so RegisterTask doesn't
	// drop duplicates when several shards of the same collection are
	// recovered.
	if !strings.Contains(t.name, "[recovery:") {
		t.name = fmt.Sprintf("%s[recovery:%s/%s]", t.name, collection, shardName)
		t.logger = t.logger.WithField("task", t.name)
	}
}

// shardReindexerV3RecoveryOnly is a stripped-down [ShardReindexerV3]
// used during startup recovery. It only fires [OnAfterLsmInit] for each
// registered task on each shard load; the heavier iteration / scheduler
// path is left to the distributed task provider so we don't bring up a
// second scheduling loop just for recovery. See
// [NewShardReindexerV3FromRecovered] for the rationale.
type shardReindexerV3RecoveryOnly struct {
	logger logrus.FieldLogger
	tasks  []*ShardReindexTaskGeneric
}

func newShardReindexerV3RecoveryOnly(logger logrus.FieldLogger) *shardReindexerV3RecoveryOnly {
	return &shardReindexerV3RecoveryOnly{
		logger: logger,
	}
}

func (r *shardReindexerV3RecoveryOnly) registerTask(t *ShardReindexTaskGeneric) {
	r.tasks = append(r.tasks, t)
}

func (r *shardReindexerV3RecoveryOnly) RunBeforeLsmInit(_ context.Context, _ *Shard) error {
	// Intentionally a no-op. The DTM's OnGroupCompleted is the
	// authoritative path for completing the swap; we don't want the
	// restart-based merge/swap in OnBeforeLsmInit to race with it.
	return nil
}

func (r *shardReindexerV3RecoveryOnly) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	if len(r.tasks) == 0 {
		return nil
	}
	for _, t := range r.tasks {
		if err := t.OnAfterLsmInit(ctx, shard); err != nil {
			r.logger.WithField("task", t.Name()).WithField("shard", shard.Name()).
				WithError(err).Error("reindex recovery: OnAfterLsmInit failed")
		}
	}
	return nil
}

func (r *shardReindexerV3RecoveryOnly) RunAfterLsmInitAsync(_ context.Context, _ *Shard) error {
	// Intentionally a no-op. See type doc.
	return nil
}

func (r *shardReindexerV3RecoveryOnly) Stop(_ *Shard, _ error) {
	// Nothing to stop — we don't run any background loops.
}
