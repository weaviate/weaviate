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
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

// reResolveStagedSwapsOnSchemaFlip converges a node that missed a semantic
// migration's swap commit because it read a PRE-flip schema at boot
// (weaviate/0-weaviate-issues#220, QA finding 2).
//
// The gap it closes: a lagging follower that STAGED + acked its swap but
// crashed BEFORE the cluster-wide schema flip applied on it reads the
// pre-flip schema during shard init. [FinalizeCompletedMigrationsWithVerdict]
// consults that schema as the verdict source, gets a false-negative
// (flipped=false), and [restoreStagedDirsToOld] reverts the tracker to the
// MERGED state (OLD restored to the canonical bucket, NEW kept at
// ingest_<gen>). Because merged.mig now exists, [ReindexProvider.LocalCallbacksDone]
// returns true, the DTM scheduler pre-marks this node's callbacks fired, and
// nothing re-drives the swap: the node serves OLD under the (soon-to-be)
// flipped schema until the NEXT restart. Self-healing on a later restart is
// an operator-timing property, not a fix.
//
// This function is the corrective re-resolve. It runs on the RAFT
// schema-apply path — [Shard.updatePropertyBuckets], invoked when the flip
// UPDATE_PROPERTY entry applies on this node — so it fires exactly when the
// verdict finally becomes durable locally. For every reverted-to-merged
// staged gen whose payload's target the just-applied schema now reflects, it
// re-drives the in-memory swap (load NEW ingest bucket → [RunSwapOnShard] →
// [CommitSwapOnShard]) so the node converges WITHOUT waiting for a restart.
//
// Why this is race-free against the DTM commit path
// ([ReindexProvider.commitStagedSwapsLocally]): the reverted-to-merged state
// is produced ONLY by [restoreStagedDirsToOld] at shard init, i.e. only on a
// node that restarted mid-window. On exactly that node, merged.mig makes
// LocalCallbacksDone return true, so the scheduler bootstrap does not re-fire
// the group callbacks — commitStagedSwapsLocally never runs for this unit.
// This hook is therefore the SOLE driver for a reverted gen; the two are
// mutually exclusive by construction.
//
// Idempotent and crash-safe. RunSwapOnShard/CommitSwapOnShard are each
// idempotent (a re-fire on an already-tidied gen is a no-op), and a crash
// mid-re-resolve leaves the gen either still reverted-merged (the flip entry
// re-applies on the next boot → this hook re-fires) or already staged/tidied
// (the next boot's finalize — now reading the flipped schema — resolves it as
// COMMIT). No timing assumptions.
//
// Best-effort: a re-resolve failure is logged, never propagated. Failing the
// schema apply would risk schema divergence, and the fallback (serve OLD
// until the next restart, which now converges deterministically because the
// schema is flipped) is strictly no worse than the pre-fix status quo.
func (s *Shard) reResolveStagedSwapsOnSchemaFlip(ctx context.Context) {
	logger := s.index.logger.WithField("action", "reindex_reresolve_on_flip").
		WithField("shard", s.name)

	lsmPath := s.pathLSM()
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	dirEntries, err := os.ReadDir(migrationsDir)
	if err != nil || len(dirEntries) == 0 {
		// ENOENT is the normal "no migrations in progress" path.
		return
	}

	className := s.index.Config.ClassName.String()
	cls := s.index.getSchema.ReadOnlyClass(className)
	if cls == nil {
		return
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		namespace, gen, ok := parseMigrationDirName(dirName)
		if !ok {
			continue
		}
		migDir := filepath.Join(migrationsDir, dirName)
		if !isRevertedMergedStagedGen(migDir) {
			continue
		}
		rec, err := readReindexRecoveryRecord(migDir)
		if err != nil {
			// No recovery payload → a legacy/format-only gen; the staged
			// protocol never applied to it.
			continue
		}
		payload := &rec.Payload
		if payload.Collection != className {
			continue
		}
		if !IsSemanticMigration(payload.MigrationType) {
			continue
		}
		// The schema is the durable verdict record: only re-drive when it
		// already reflects THIS payload's target (COMMIT). A gen whose
		// sibling property flips have not yet applied stays parked until
		// they do — semanticMigrationSchemaFlipped requires every property.
		flipped, vok := semanticMigrationSchemaFlipped(cls, payload)
		if !vok || !flipped {
			continue
		}

		props, err := readMigrationProps(migDir)
		if err != nil || len(props) == 0 {
			continue
		}
		task := buildStagedReResolveTask(s.index.logger, namespace, gen, props, payload)
		if task == nil {
			// Not a property-level staged strategy we can re-drive from
			// disk here (e.g. class-level change-algorithm, whose flip is
			// an UPDATE_CLASS and reaches a different apply path).
			continue
		}

		if err := s.driveStagedSwapReResolve(ctx, logger, task, dirName, props); err != nil {
			logger.WithField("migration", dirName).
				Errorf("reindex re-resolve: could not converge staged swap on flip apply; the node serves OLD until the next restart's finalize (which now reads the flipped schema and commits): %v", err)
		}
	}
}

// driveStagedSwapReResolve loads the NEW ingest bucket (so RunSwapOnShard
// takes the crash-safe in-memory SwapBucketPointer path rather than the
// live-bucket dir-rename recovery path), then stages and commits the swap.
// The schema is already flipped at this point, so no tokenization overlay is
// needed: the swapped-in NEW bucket matches the applied schema immediately.
func (s *Shard) driveStagedSwapReResolve(
	ctx context.Context, logger logrus.FieldLogger,
	task *ShardReindexTaskGeneric, dirName string, props []string,
) error {
	logger = logger.WithField("migration", dirName)
	logger.Info("reindex re-resolve: schema flip applied while this gen was reverted to merged; re-driving staged swap to converge without a restart")

	// The reverted-to-merged state leaves the NEW data at ingest_<gen> but
	// unloaded. Load it before RunSwapOnShard so its IsMerged branch does the
	// in-memory pointer flip instead of renaming the LIVE canonical bucket's
	// dir (which would corrupt the store).
	if err := task.ensureReindexBucketsLoadedForSwap(ctx, logger, s, props); err != nil {
		return err
	}
	if err := task.RunSwapOnShard(ctx, s); err != nil {
		return err
	}
	if err := task.CommitSwapOnShard(ctx, s); err != nil {
		return err
	}
	logger.Info("reindex re-resolve: staged swap committed on flip apply; node converged to NEW under the flipped schema")
	return nil
}

// isRevertedMergedStagedGen reports whether a migration tracker dir is in the
// reverted-to-merged state produced by [restoreStagedDirsToOld]: merged.mig
// present, and none of the swap/stage/commit/rollback sentinels. A gen that
// was never swapped (fresh merged) matches the same on-disk shape, but at
// flip-apply time that is indistinguishable from — and treated identically
// to — a reverted gen: the ack barrier only lets the flip commit after every
// unit staged, so a merged (unswapped) gen on this node at flip-apply means
// this node reverted. Re-driving either to NEW under an already-flipped
// schema is correct.
func isRevertedMergedStagedGen(migDir string) bool {
	return fileExists(filepath.Join(migDir, "merged.mig")) &&
		!fileExists(filepath.Join(migDir, "staged.mig")) &&
		!fileExists(filepath.Join(migDir, "swapped.mig")) &&
		!fileExists(filepath.Join(migDir, "tidied.mig")) &&
		!fileExists(filepath.Join(migDir, "unswapped.mig"))
}

// buildStagedReResolveTask reconstructs the ShardReindexTaskGeneric for a
// reverted staged gen from its on-disk tracker namespace + recovery payload,
// so [driveStagedSwapReResolve] can re-run the swap without the
// ReindexProvider (unreachable from the DB-layer schema-apply path). Mirrors
// the property-level staging arms of [ReindexProvider.createReindexTasks];
// returns nil for any strategy that does not stage a per-property swap
// reachable via updatePropertyBuckets (class-level change-algorithm, the
// format-only strategies), which the caller then skips.
func buildStagedReResolveTask(
	logger logrus.FieldLogger, namespace string, gen int, props []string, payload *ReindexTaskPayload,
) *ShardReindexTaskGeneric {
	switch {
	case strings.HasPrefix(namespace, MigrationDirPrefixSearchableRetokenize):
		if len(props) != 1 || payload.TargetTokenization == "" || payload.BucketStrategy == "" {
			return nil
		}
		return NewRuntimeSearchableRetokenizeTask(logger, props[0], payload.TargetTokenization,
			payload.Collection, payload.BucketStrategy, payload.Collection, gen)
	case strings.HasPrefix(namespace, MigrationDirPrefixFilterableRetokenize):
		if len(props) != 1 || payload.TargetTokenization == "" {
			return nil
		}
		return NewRuntimeFilterableRetokenizeTask(logger, props[0], payload.TargetTokenization,
			payload.Collection, payload.Collection, gen)
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableSearchable):
		if payload.TargetTokenization == "" {
			return nil
		}
		return NewRuntimeEnableSearchableTask(logger, props, payload.Collection, payload.TargetTokenization, gen)
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableFilterable):
		return NewRuntimeEnableFilterableTask(logger, props, payload.Collection, gen)
	default:
		return nil
	}
}
