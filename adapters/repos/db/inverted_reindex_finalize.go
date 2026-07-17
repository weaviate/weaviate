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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// nextMigrationGeneration returns the per-node generation `N` a new
// migration on (migrationDirPrefix, propNamesSuffix) should use on this
// shard's LSM directory. The new migration writes to dirs suffixed
// `_<N>`; older generations (if any) still live alongside the
// canonical main bucket until [FinalizeCompletedMigrations] runs at
// next startup.
//
// `migrationDirPrefix` is one of the constants in
// inverted_reindex_strategy_dir_names.go (e.g. `searchable_retokenize`
// or `searchable_map_to_blockmax`). `propNamesSuffix` is the
// strategy-specific per-property tail (e.g. `_text` for the per-property
// retokenize strategies, or the sorted-joined "_p1_p2" for multi-property
// strategies — pass "" for class-level strategies). The full dir name
// pattern matched is `<migrationDirPrefix><propNamesSuffix>_<N>`.
//
// Returns 1 when no prior generation exists. Returns max(existing)+1
// otherwise. Non-integer-suffixed dirs (i.e. pre-generation legacy
// state, which shouldn't exist on this branch but defensive code is
// cheap) are ignored.
//
// Called from [ReindexProvider.processOneUnit] before constructing the
// strategy instance, once per shard / prop / indexType tuple. Computed
// per-node — different nodes may pick different generations for the
// same RAFT task and that's correct: generation is purely a per-node
// on-disk implementation detail of the deferred-finalize design.
func nextMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix string) int {
	return maxMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix) + 1
}

// MaxMigrationGenerationForDebug is an exported wrapper around
// [maxMigrationGeneration] for the REST debug handlers. Production code
// should use [maxMigrationGeneration] / [nextMigrationGeneration]
// directly.
func MaxMigrationGenerationForDebug(lsmPath, migrationDirPrefix, propNamesSuffix string) int {
	return maxMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix)
}

// GenSuffixForDebug is an exported wrapper around [genSuffix] for the
// REST debug handlers. Production code should use [genSuffix] directly.
func GenSuffixForDebug(generation int) string {
	return genSuffix(generation)
}

// maxMigrationGeneration returns the highest existing generation on disk
// for the (prefix, propNamesSuffix) tuple, or 0 if none exists.
//
// Used by recovery / rehydrate paths that need to construct a strategy
// instance matching an existing on-disk migration. The recovery path is
// the only legitimate caller — fresh task starts should always use
// [nextMigrationGeneration] to claim a new generation.
func maxMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix string) int {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return 0
	}
	target := migrationDirPrefix + propNamesSuffix
	highest := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		prefix, gen, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		if prefix != target {
			continue
		}
		if gen > highest {
			highest = gen
		}
	}
	return highest
}

// completedMigrationGens returns the set of generation numbers whose
// migration tracker dir (for any of the strategy prefixes in `prefixes`)
// has `tidied.mig` or `merged.mig` on disk — i.e., migrations that
// completed successfully in-process and whose sidecar dirs are LIVE data
// pointed at by the in-memory bucket pointers, awaiting next-restart
// finalize to be promoted to canonical names.
//
// Called from the submit-handler and cancel-handler pre-submit cleanup
// path ([Shard.CleanStalePartialReindexState]) so the cleanup can skip
// tracker and sidecar dirs that belong to a completed-but-deferred
// migration on the same property. Without this gate, a back-to-back
// submit-without-restart sequence wipes the prior completed migration's
// live ingest dir out from under its in-memory bucket pointer → the
// canonical bucket becomes empty → silent #10675-shape data loss on the
// submitting node.
//
// `prefixes` is the strategy-dir prefixes from
// [migrationDirsForPropertyIndex] for the (propName, indexType) tuple.
func completedMigrationGens(lsmPath string, prefixes []string) map[int]bool {
	out := map[int]bool{}
	forEachCompletedMigration(lsmPath, prefixes, func(base string, gen int) {
		out[gen] = true
	})
	return out
}

// completedMigrationSidecarSuffixes returns the gen-suffixed sidecar dir
// suffixes (e.g. "__roaringset_ingest_2") owned by completed-but-deferred
// migrations matching `prefixes`. Keying by (suffix-base, gen) instead of
// bare gen stops one strategy's completed gen from shielding — or failing
// to shield — a different strategy's sidecar at the same gen (issue #295).
func completedMigrationSidecarSuffixes(lsmPath string, prefixes []string) map[string]bool {
	out := map[string]bool{}
	forEachCompletedMigration(lsmPath, prefixes, func(base string, gen int) {
		suffixes := migrationSuffixes(base)
		if suffixes == nil {
			return
		}
		tail := genSuffix(gen)
		out[suffixes.ingestSuffix+tail] = true
		out[suffixes.backupSuffix+tail] = true
		if rs := reindexSuffixForFinalize(base); rs != "" {
			out[rs+tail] = true
		}
	})
	return out
}

// forEachCompletedMigration invokes fn for every tracker dir under
// lsmPath/.migrations matching `prefixes` that carries tidied.mig or
// merged.mig (completed in-process, awaiting next-restart finalize).
func forEachCompletedMigration(lsmPath string, prefixes []string, fn func(base string, gen int)) {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return
	}
	prefixSet := map[string]bool{}
	for _, p := range prefixes {
		prefixSet[p] = true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, gen, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		if !prefixSet[base] {
			continue
		}
		dirPath := filepath.Join(migrationsDir, entry.Name())
		if fileExistsInDir(dirPath, "tidied.mig") || fileExistsInDir(dirPath, "merged.mig") {
			fn(base, gen)
		}
	}
}

// fileExistsInDir is a small helper for [completedMigrationGens]; returns
// true iff the named file is present in dirPath as a regular file.
func fileExistsInDir(dirPath, fileName string) bool {
	info, err := os.Stat(filepath.Join(dirPath, fileName))
	return err == nil && !info.IsDir()
}

// StagedMigrationVerdictFunc resolves the cluster verdict for a
// staged-but-uncommitted semantic swap at startup
// (weaviate/0-weaviate-issues#220). The schema is the verdict record —
// the flip only commits via RAFT once the ack barrier confirmed every
// unit — so comparing the schema against the migration's target answers
// "did the task commit?" without needing DTM state, unavailable this
// early in shard init. flipped=true: COMMIT (promote NEW); flipped=false:
// pending/FAILED (restore OLD); ok=false: schema unavailable.
type StagedMigrationVerdictFunc func(payload *ReindexTaskPayload) (flipped bool, ok bool)

// FinalizeCompletedMigrations scans the shard's .migrations/ directory for
// completed migrations that still need filesystem cleanup, and runs the
// deferred ingest→canonical rename for each.
//
// Every migration tracker dir on disk carries a per-node generation
// suffix `_<N>` (see [genSuffix]). For each (prop, indexType) tuple
// there may be multiple generations on disk if the prior end-of-swap
// trim hadn't run yet — for example because the process crashed between
// `markTidied` and the per-shard trim, or because a follow-up migration
// is in flight at gen > latest_tidied.
//
// Algorithm, per namespace (the strategy-prefix + props-suffix returned
// by [parseMigrationDirName]):
//
//   - Find the highest gen `T` with `tidied.mig` present.
//   - Find the highest gen `M` with `merged.mig` present (regardless of
//     tidied). `merged.mig` means the reindex iteration completed and its
//     segments were prepended into the ingest bucket — i.e. the ingest
//     dir on disk holds the complete dataset under the target tokenization.
//     `tidied.mig` is only set later (after the in-memory bucket pointer
//     swap and the per-prop old-main→backup directory rename); if the
//     runtime swap failed between `markMerged` and `markTidied`, we have
//     `merged.mig` without `tidied.mig`.
//   - effective = max(T, M).
//   - If `effective` exists:
//   - If `effective == T`: standard path. Finalize `T`: rename
//     `…_<ingestSuffix-base>_<T>/` → canonical
//     `property_<prop>_<index>/`, remove `…_<backupSuffix-base>_<T>/`.
//   - If `effective == M > T`: recovery path. The in-process runtime
//     swap on this node crashed AFTER `markMerged` but BEFORE
//     `markTidied`, so the ingest dir at gen M holds the
//     target-tokenization data the schema expects and is safe to
//     promote even though the canonical-name rename never ran. Write
//     `swapped.mig` + `tidied.mig` sentinels into gen-M's tracker dir
//     (so the namespace becomes self-consistent on disk and the same
//     finalize path runs) and then promote gen M the same way.
//     CRITICAL: this means the cluster-wide schema flip
//     [ReindexProvider.flipSemanticMigrationSchema] has likely already
//     committed via RAFT (the DTM task was FINISHED before this node
//     died, otherwise the unit would not have transitioned terminal),
//     so the canonical bucket MUST have target-tokenization data on
//     restart — otherwise this node serves the old data under the new
//     schema → divergence vs other replicas → #10675-shape bug.
//   - Remove every dir on disk (sidecars + tracker) with gen < effective
//     — these are pre-`effective` data, no longer referenced.
//   - Remove the tracker dir for `effective` itself.
//   - If neither `T` nor `M` exists, do nothing — any earlier-stage
//     in-flight migration on disk is the recovery path's
//     responsibility ([DiscoverInFlightReindexTasks]).
//   - Generations with `gen > effective` are in-flight (next migration)
//     and left alone — recovery picks them up via their `payload.mig`.
//
// CRITICAL: This MUST be called BEFORE bucket loading, NEVER on live
// buckets. Renaming directories while buckets are open would corrupt
// the store. The deferred-finalize design relies on the in-memory swap
// (via DTM) marking tidied while the directory renames are deferred to
// the next startup when no buckets are loaded. See
// `docs/runtime-reindex.md` for the rationale.
func FinalizeCompletedMigrations(lsmPath string, logger logrus.FieldLogger) {
	FinalizeCompletedMigrationsWithVerdict(lsmPath, logger, nil)
}

// FinalizeCompletedMigrationsWithVerdict is [FinalizeCompletedMigrations]
// plus staged-window crash recovery (weaviate/0-weaviate-issues#220): a
// STAGED gen (staged.mig, or a semantic migration's
// merged-but-unverdicted state) is resolved via verdict rather than
// blindly promoted — see [resolveStagedSwapAtStartup] for the two
// outcomes. Gens carrying unswapped.mig (a deferred rollback restore)
// are handled by [restoreRolledBackStagedSwap]. The nil-verdict wrapper
// above is the least-destructive default: treat every staged gen as
// unresolved.
func FinalizeCompletedMigrationsWithVerdict(lsmPath string, logger logrus.FieldLogger, verdict StagedMigrationVerdictFunc) {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		if !os.IsNotExist(err) {
			// ENOENT is the normal "no migrations in progress" path; anything
			// else (EACCES, EIO, etc.) is worth surfacing so an operator can
			// notice that pending finalizations are being silently skipped.
			logger.WithField("path", migrationsDir).
				Warnf("reindex finalize: unable to read migrations dir; pending finalizations skipped: %v", err)
		}
		return
	}

	// Group entries by namespace (prefix returned by parseMigrationDirName).
	// Within each namespace, find the highest tidied gen and any lower
	// gens to clean up. Higher (untidied) gens are deferred to recovery
	// EXCEPT when they have merged.mig — see the recovery path below.
	groups := map[string][]genInfo{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		namespace, gen, ok := parseMigrationDirName(name)
		if !ok {
			// Entry doesn't follow the `<prefix>_<N>` convention. Skip —
			// this branch never produces such entries; defensive.
			continue
		}
		dirPath := filepath.Join(migrationsDir, name)
		groups[namespace] = append(groups[namespace], genInfo{
			dirName:   name,
			gen:       gen,
			tidied:    fileExists(filepath.Join(dirPath, "tidied.mig")),
			merged:    fileExists(filepath.Join(dirPath, "merged.mig")),
			staged:    fileExists(filepath.Join(dirPath, "staged.mig")),
			unswapped: fileExists(filepath.Join(dirPath, "unswapped.mig")),
		})
	}

	for namespace, gens := range groups {
		// Staged-window pre-pass: resolve rolled-back/staged gens before
		// promotion candidates are computed, so an unverdicted swap can
		// never be promoted by the generic paths below.
		pending := map[int]bool{}
		for i := range gens {
			g := gens[i]
			migDir := filepath.Join(migrationsDir, g.dirName)
			switch {
			case g.unswapped:
				restoreRolledBackStagedSwap(lsmPath, migDir, g.dirName, namespace, logger)
				pending[g.gen] = true
			case !g.tidied && (g.staged || isUnverdictedSemanticGen(migDir, g)):
				if resolveStagedSwapAtStartup(lsmPath, migDir, g.dirName, namespace, verdict, logger) {
					// COMMIT direction: the tidied sentinels were written;
					// the gen is now an ordinary promotion candidate.
					gens[i].tidied = true
				} else {
					pending[g.gen] = true
				}
			}
		}
		// Find the highest tidied gen and the highest merged gen.
		// The "effective" promotion candidate is the larger of the two
		// — see the godoc on FinalizeCompletedMigrations for why a
		// merged-but-not-tidied gen is safe (and required) to promote.
		// Gens parked by the staged-window pre-pass are not candidates.
		highestTidied := -1
		highestMerged := -1
		for _, g := range gens {
			if pending[g.gen] {
				continue
			}
			if g.tidied && g.gen > highestTidied {
				highestTidied = g.gen
			}
			if g.merged && g.gen > highestMerged {
				highestMerged = g.gen
			}
		}
		effective := highestTidied
		if highestMerged > effective {
			effective = highestMerged
		}
		if effective < 0 {
			// No tidied or merged migration in this namespace — recovery
			// owns any earlier-stage in-flight state. Move on.
			continue
		}

		// If the effective promotion gen lacks tidied.mig, this is the
		// recovery path: the in-process runtime swap on this node died
		// after markMerged but before markTidied. Write the missing
		// sentinels so the rest of the finalize logic sees a consistent
		// tracker and the same ingest→canonical rename runs. The schema
		// flip has likely already committed cluster-wide via the DTM
		// task's FINISHED state; promoting gen-effective here is what
		// makes this node's bucket data consistent with that schema.
		if effective > highestTidied {
			for _, g := range gens {
				if g.gen != effective {
					continue
				}
				migDir := filepath.Join(migrationsDir, g.dirName)
				if err := writeRecoveryTidiedSentinels(migDir); err != nil {
					logger.WithField("migration", g.dirName).
						Errorf("reindex finalize: failed to write recovery tidied sentinels; this node may end up with stale data after restart: %v", err)
					// Skip the recovery path; fall back to the tidied
					// gen if any (existing behavior).
					effective = highestTidied
				} else {
					logger.WithField("migration", g.dirName).WithField("gen", effective).
						Info("reindex finalize: recovered untidied gen — runtime swap died post-merge, completing finalize from disk state")
				}
				break
			}
			if effective < 0 {
				continue
			}
		}

		// Finalize the effective promotion gen, then remove every gen <
		// effective (their data was superseded by this gen's complete
		// or recovered ingest dir).
		for _, g := range gens {
			if pending[g.gen] {
				// Parked by the staged-window pre-pass — never promote or
				// delete these here.
				continue
			}
			migDir := filepath.Join(migrationsDir, g.dirName)
			switch {
			case g.gen == effective:
				finalizeMigrationDir(lsmPath, migDir, g.dirName, logger)
				// finalizeMigrationDir performs the ingest→canonical
				// rename + backup removal. We also remove the tracker
				// dir itself: its sentinels have done their job.
				if err := os.RemoveAll(migDir); err != nil {
					logger.WithField("path", migDir).
						Warnf("reindex finalize: failed to remove finalized tracker dir: %v", err)
				}
			case g.gen < effective:
				// Stale older gen: remove tracker dir AND its sidecar
				// dirs (their backup/ingest/reindex dirs on disk are
				// orphaned by the newer migration's swap, OR — in the
				// recovery path — they are the previous gen's old live
				// main that the failed swap never renamed to backup;
				// either way they're stale relative to the effective
				// gen's promoted data).
				removeStaleSidecarsForGen(lsmPath, namespace, g.dirName, logger)
				if err := os.RemoveAll(migDir); err != nil {
					logger.WithField("path", migDir).
						Warnf("reindex finalize: failed to remove stale older-gen tracker dir: %v", err)
				}
			default:
				// gen > effective: even-earlier in-flight (e.g. crashed
				// before markMerged); recovery handles via its own
				// payload.mig read.
			}
		}
	}
}

// genInfo captures one migration tracker dir's sentinel state for
// [FinalizeCompletedMigrationsWithVerdict]'s per-namespace walk.
type genInfo struct {
	dirName   string
	gen       int
	tidied    bool
	merged    bool
	staged    bool
	unswapped bool
}

// isUnverdictedSemanticGen reports whether a merged-but-untidied gen is
// a semantic migration whose cluster verdict is unknown at boot — such
// gens must skip the generic merged-promotion recovery path, whose "the
// schema flip already committed" assumption is wrong when a sibling unit
// failed (weaviate/0-weaviate-issues#220). A gen without payload.mig is
// a legacy/format-only flow and keeps the old behavior.
func isUnverdictedSemanticGen(migDir string, g genInfo) bool {
	if !g.merged {
		return false
	}
	rec, err := readReindexRecoveryRecord(migDir)
	if err != nil {
		return false
	}
	return IsSemanticMigration(rec.Payload.MigrationType)
}

// readReindexRecoveryRecord loads the payload.mig recovery record from a
// migration tracker dir.
func readReindexRecoveryRecord(migDir string) (*reindexRecoveryRecord, error) {
	data, err := os.ReadFile(filepath.Join(migDir, reindexRecoveryPayloadFile))
	if err != nil {
		return nil, err
	}
	var rec reindexRecoveryRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", reindexRecoveryPayloadFile, err)
	}
	return &rec, nil
}

// resolveStagedSwapAtStartup resolves one staged (or semantic
// merged-but-unverdicted) generation against the cluster verdict at
// startup. Returns true when the verdict is COMMIT and the gen was
// upgraded to an ordinary promotion candidate (tidied sentinels
// written); false when the gen was parked (restored to OLD / left
// merged) and must be excluded from promotion and cleanup this pass.
//
// Runs pre-bucket-load, so directory renames are safe.
func resolveStagedSwapAtStartup(
	lsmPath, migDir, dirName, namespace string,
	verdict StagedMigrationVerdictFunc,
	logger logrus.FieldLogger,
) bool {
	flipped := false
	rec, err := readReindexRecoveryRecord(migDir)
	switch {
	case err != nil:
		logger.WithField("migration", dirName).
			Errorf("reindex finalize: staged swap with unreadable recovery payload; conservatively restoring OLD data (nothing is deleted — the NEW ingest data stays staged on disk): %v", err)
	case verdict == nil:
		logger.WithField("migration", dirName).
			Warn("reindex finalize: staged swap but no verdict source wired; conservatively restoring OLD data")
	default:
		var ok bool
		flipped, ok = verdict(&rec.Payload)
		if !ok {
			logger.WithField("migration", dirName).
				Errorf("reindex finalize: staged swap for %q but the schema cannot be consulted; conservatively restoring OLD data", rec.Payload.Collection)
			flipped = false
		}
	}

	if flipped {
		// COMMIT: the cluster-wide schema flip landed before this node's
		// local commit finished. Upgrade the gen to a normal promotion
		// candidate; the caller's finalize pass renames ingest→canonical
		// and drops the OLD backup + tracker.
		if err := writeRecoveryTidiedSentinels(migDir); err != nil {
			logger.WithField("migration", dirName).
				Errorf("reindex finalize: staged swap verdict is COMMIT but tidied sentinels could not be written; leaving staged (this node serves OLD under the flipped schema until repaired): %v", err)
			return false
		}
		if err := os.Remove(filepath.Join(migDir, "staged.mig")); err != nil && !os.IsNotExist(err) {
			logger.WithField("migration", dirName).
				Warnf("reindex finalize: failed to remove staged.mig after commit resolution: %v", err)
		}
		logger.WithField("migration", dirName).
			Info("reindex finalize: staged swap resolved as COMMIT (schema already flipped); promoting staged data")
		return true
	}

	// Verdict pending or FAILED: restore OLD as canonical and revert the
	// tracker to the merged state. The NEW ingest data and the tracker
	// stay so a still-live task can re-stage via the DTM re-drive; a
	// FAILED task's leftovers are inert.
	restoreStagedDirsToOld(lsmPath, migDir, dirName, namespace, logger)
	logger.WithField("migration", dirName).
		Info("reindex finalize: staged swap resolved as PENDING/FAILED (schema not flipped); OLD data restored to canonical, tracker reverted to merged")
	return false
}

// migrationDirCoords resolves the two silently-guarded per-property inputs
// every finalize/restore walk needs from a migration dir name: the strategy's
// bucket-name suffixes and the `_<gen>` dir-name tail. ok=false (caller bails
// silently, matching pre-refactor behavior) when the strategy prefix is
// unknown or the name carries no gen suffix — both defensive guards against
// dirs this branch never writes. The gen tail is reconstructed by re-appending
// `_<N>` rather than asking the strategy instance (finalize does not have one),
// which is exact because every strategy's suffix methods compose the on-disk
// name as `<base>_<N>`. Safe to run before the properties.mig read: at every
// real call site parseMigrationDirName already succeeded upstream in
// [FinalizeCompletedMigrationsWithVerdict]'s grouping, so this re-parse never
// fails and cannot reorder an observable early return.
func migrationDirCoords(dirName string) (suffixes *migrationBucketSuffixes, genTail string, ok bool) {
	suffixes = migrationSuffixes(dirName)
	if suffixes == nil {
		return nil, "", false
	}
	_, gen, ok := parseMigrationDirName(dirName)
	if !ok {
		return nil, "", false
	}
	return suffixes, "_" + strconv.Itoa(gen), true
}

// restoreStagedDirsToOld restores the OLD data to the canonical bucket
// dir for every prop of a staged gen and clears the swap/staged
// sentinels, reverting the tracker to the "merged" state. Handles both
// staged layouts: the deferred-rename layout (canonical dir absent, OLD
// at backup_<gen>, NEW at ingest_<gen>) and the dir-rename layout from
// [recoverRuntimeSwapBuckets] (NEW at the canonical name, OLD at
// backup_<gen>, ingest absent — NEW is moved back to ingest_<gen>).
// Presence-checked and idempotent: a crash mid-restore re-converges on
// the next pass.
func restoreStagedDirsToOld(lsmPath, migDir, dirName, namespace string, logger logrus.FieldLogger) {
	suffixes, genTail, ok := migrationDirCoords(dirName)
	if !ok {
		return
	}
	props, err := readMigrationProps(migDir)
	if err != nil {
		logger.WithField("migration", dirName).
			Errorf("reindex finalize: staged restore: properties.mig unreadable; leaving dirs untouched: %v", err)
		return
	}

	for _, propName := range props {
		mainDir := filepath.Join(lsmPath, suffixes.sourceBucketName(propName))
		backupDir := filepath.Join(lsmPath, suffixes.sourceBucketName(propName)+suffixes.backupSuffix+genTail)
		ingestDir := filepath.Join(lsmPath, suffixes.sourceBucketName(propName)+suffixes.ingestSuffix+genTail)

		if !fileExists(backupDir) {
			// Already restored on a prior pass, or this prop never
			// swapped. Nothing to do either way.
			continue
		}
		if fileExists(mainDir) {
			// Dir-rename layout: the canonical name currently holds NEW.
			// Move it back to the ingest slot so OLD can take the
			// canonical name.
			if fileExists(ingestDir) {
				// Ambiguous: NEW exists under both names. Should be
				// impossible (the dir-rename swap consumed the ingest
				// dir); refuse to guess.
				logger.WithField("migration", dirName).WithField("property", propName).
					Errorf("reindex finalize: staged restore: canonical, backup AND ingest dirs all present — ambiguous layout, skipping prop")
				continue
			}
			if err := os.Rename(mainDir, ingestDir); err != nil {
				logger.WithField("migration", dirName).WithField("property", propName).
					Errorf("reindex finalize: staged restore: failed to move NEW data off the canonical name: %v", err)
				continue
			}
		}
		if err := os.Rename(backupDir, mainDir); err != nil {
			logger.WithField("migration", dirName).WithField("property", propName).
				Errorf("reindex finalize: staged restore: failed to restore OLD data to the canonical name: %v", err)
			continue
		}
	}

	// Revert the tracker to "merged": clear the global + per-prop swapped
	// sentinels and the staged marker. merged.mig, properties.mig and
	// payload.mig stay for the DTM re-drive.
	trackerEntries, err := os.ReadDir(migDir)
	if err != nil {
		logger.WithField("migration", dirName).
			Warnf("reindex finalize: staged restore: cannot list tracker dir for sentinel cleanup: %v", err)
		return
	}
	for _, e := range trackerEntries {
		name := e.Name()
		if name == "swapped.mig" || name == "staged.mig" || strings.HasPrefix(name, "swapped.mig.") {
			if err := os.Remove(filepath.Join(migDir, name)); err != nil {
				logger.WithField("migration", dirName).
					Warnf("reindex finalize: staged restore: failed to remove sentinel %s: %v", name, err)
			}
		}
	}

	// Durably record that THIS gen reached merged-only via the boot-revert
	// path (0-weaviate-issues#220). The flip-apply re-resolve hook requires
	// this positive evidence before re-driving a merged-only gen: a gen that
	// reached merged-only by failing mid-swap is on-disk identical in every
	// other sentinel but never ran this revert, so it lacks the marker and
	// the hook leaves it (and its OLD backup) untouched. Written last and
	// fsync-durable so a crash before it lands simply re-runs this revert on
	// the next boot (idempotent) rather than mis-classifying the gen. A
	// failure to persist it is non-fatal: the hook then declines to re-drive
	// (the gen converges on the next restart's finalize instead), which is
	// strictly no worse than the pre-hook status quo.
	if err := createFileDurableAt(migDir, reindexRevertedMarkerFile, nil); err != nil {
		logger.WithField("migration", dirName).
			Warnf("reindex finalize: staged restore: failed to write revert marker %s; flip-apply re-resolve will defer to the next restart: %v", reindexRevertedMarkerFile, err)
	}
}

// restoreRolledBackStagedSwap completes the on-disk half of an
// in-process rollback ([ShardReindexTaskGeneric.RollbackSwapOnShard]):
// the OLD data still lives under its backup-named dir (it was the live
// bucket when the rollback ran, so the rename had to wait for a
// restart). Restores it to the canonical name, discards the migration's
// remaining sidecar dirs and removes the tracker — the task is terminal,
// nothing re-drives this gen.
func restoreRolledBackStagedSwap(lsmPath, migDir, dirName, namespace string, logger logrus.FieldLogger) {
	suffixes, genTail, ok := migrationDirCoords(dirName)
	if !ok {
		return
	}
	props, err := readMigrationProps(migDir)
	if err != nil {
		logger.WithField("migration", dirName).
			Errorf("reindex finalize: rollback restore: properties.mig unreadable; leaving dirs untouched: %v", err)
		return
	}

	for _, propName := range props {
		mainDir := filepath.Join(lsmPath, suffixes.sourceBucketName(propName))
		backupDir := filepath.Join(lsmPath, suffixes.sourceBucketName(propName)+suffixes.backupSuffix+genTail)
		if !fileExists(mainDir) && fileExists(backupDir) {
			if err := os.Rename(backupDir, mainDir); err != nil {
				logger.WithField("migration", dirName).WithField("property", propName).
					Errorf("reindex finalize: rollback restore: failed to restore OLD data to the canonical name; tracker kept for a retry next restart: %v", err)
				return
			}
		}
		// Discard the failed migration's remaining sidecars (the in-process
		// rollback already removed the ingest dirs; presence-checked).
		for _, suff := range []string{suffixes.ingestSuffix + genTail, reindexSuffixForFinalize(namespace) + genTail} {
			p := filepath.Join(lsmPath, suffixes.sourceBucketName(propName)+suff)
			if fileExists(p) {
				if err := os.RemoveAll(p); err != nil {
					logger.WithField("path", p).
						Warnf("reindex finalize: rollback restore: failed to remove discarded sidecar dir: %v", err)
				}
			}
		}
	}
	if err := os.RemoveAll(migDir); err != nil {
		logger.WithField("path", migDir).
			Warnf("reindex finalize: rollback restore: failed to remove tracker dir: %v", err)
	}
	logger.WithField("migration", dirName).
		Info("reindex finalize: completed on-disk restore of a rolled-back staged swap")
}

// writeRecoveryTidiedSentinels is the recovery-path equivalent of the
// per-prop swapped.mig writes that runtimeSwap step 3 emits plus the
// global swapped.mig and tidied.mig writes that come right after. It is
// called at startup only, when the on-disk state shows merged.mig but
// neither swapped.mig nor tidied.mig — i.e. the runtime swap crashed
// after `markMerged` and before completing the per-prop directory
// renames. The tracker carries `merged.mig` which means the prepend
// step finished and the ingest dir holds a complete, target-tokenization
// dataset; FinalizeCompletedMigrations needs the swapped/tidied
// sentinels in order to drive its existing ingest→canonical rename
// path. Writing them retroactively is safe because no buckets are
// loaded yet (we are pre-shard-init) and the underlying invariant
// (ingest dir holds the right data) has been verified by the
// `markMerged` semantics. We do NOT write swapped-per-prop sentinels
// because the existing finalize loop does not consume them.
func writeRecoveryTidiedSentinels(migDir string) error {
	for _, name := range []string{"swapped.mig", "tidied.mig"} {
		p := filepath.Join(migDir, name)
		if fileExists(p) {
			continue
		}
		f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", name, err)
		}
	}
	return nil
}

// removeStaleSidecarsForGen removes the `__<...>_<gen>` sidecar dirs
// (reindex/ingest/backup) belonging to an older, superseded generation
// of a finalized migration. Looks up the per-strategy suffix bases via
// `migrationSuffixes` (which now returns the suffix bases without the
// `_<N>` part) and removes any matching dir for the specific `_<gen>`.
//
// Props are read from the older gen's `properties.mig` (or recovered
// from the on-disk dirs themselves if properties.mig is missing — the
// latter is defensive against partial pre-migration state).
func removeStaleSidecarsForGen(lsmPath, namespace, dirName string, logger logrus.FieldLogger) {
	migDir := filepath.Join(lsmPath, ".migrations", dirName)
	suffixes, genTail, ok := migrationDirCoords(dirName)
	if !ok {
		return
	}
	props, err := readMigrationProps(migDir)
	if err != nil {
		logger.WithField("path", migDir).
			Debugf("reindex finalize: stale-gen cleanup: properties.mig missing/unreadable; sidecars (if any) will be left as orphans: %v", err)
		return
	}
	for _, propName := range props {
		main := suffixes.sourceBucketName(propName)
		for _, suff := range []string{suffixes.ingestSuffix, suffixes.backupSuffix, reindexSuffixForFinalize(namespace)} {
			path := filepath.Join(lsmPath, main+suff+genTail)
			if fileExists(path) {
				if err := os.RemoveAll(path); err != nil {
					logger.WithField("path", path).
						Warnf("reindex finalize: failed to remove stale older-gen sidecar dir: %v", err)
				}
			}
		}
	}
}

// reindexSuffixForFinalize returns the per-strategy reindex bucket
// suffix base (e.g. `__retokenize_reindex`) used to identify older-gen
// reindex sidecar dirs in the finalize cleanup. Kept in lockstep with
// each strategy's ReindexSuffix() base — when a new strategy is added,
// extend both this switch and the strategy's ReindexSuffix() method.
func reindexSuffixForFinalize(namespace string) string {
	switch {
	case strings.HasPrefix(namespace, MigrationDirSearchableMapToBlockmax):
		return "__blockmax_reindex"
	case strings.HasPrefix(namespace, MigrationDirFilterableRoaringsetRefresh):
		return "__roaringset_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixFilterableToRangeable):
		return "__rangeable_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixSearchableRetokenize):
		return "__retokenize_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixFilterableRetokenize):
		return "__filt_retokenize_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableFilterable):
		return "__enable_filterable_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableSearchable):
		return "__enable_searchable_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixRebuildSearchable):
		return "__rebuild_searchable_reindex"
	}
	return ""
}

func finalizeMigrationDir(lsmPath, migDir, migName string, logger logrus.FieldLogger) {
	// Only finalize if both swapped and tidied sentinels exist.
	if !fileExists(filepath.Join(migDir, "swapped.mig")) {
		return
	}
	if !fileExists(filepath.Join(migDir, "tidied.mig")) {
		return
	}

	// Read properties from the migration.
	props, err := readMigrationProps(migDir)
	if err != nil || len(props) == 0 {
		return
	}

	// Determine bucket naming from migration dir name. The migration dir
	// name carries a `_<gen>` suffix (e.g. `searchable_retokenize_text_2`);
	// the strategy's IngestSuffix / BackupSuffix methods on the writer
	// side appended the same gen to the suffix base. Reproduce that here
	// to find the matching on-disk sidecar dirs.
	suffixes, genTail, ok := migrationDirCoords(migName)
	if !ok {
		return
	}

	logger = logger.WithField("migration", migName)

	for _, propName := range props {
		mainName := suffixes.sourceBucketName(propName)
		ingestDir := filepath.Join(lsmPath, mainName+suffixes.ingestSuffix+genTail)
		backupDir := filepath.Join(lsmPath, mainName+suffixes.backupSuffix+genTail)
		mainDir := filepath.Join(lsmPath, mainName)

		// Remove backup dir.
		if fileExists(backupDir) {
			if err := os.RemoveAll(backupDir); err != nil {
				logger.WithField("dir", backupDir).
					Errorf("finalize: failed to remove backup dir: %v", err)
				continue
			}
			logger.WithField("dir", backupDir).Debug("finalize: removed backup dir")
		}

		// Rename ingest dir to canonical main dir.
		if fileExists(ingestDir) {
			// Remove stale main dir if it exists (shouldn't normally, but be safe).
			if fileExists(mainDir) {
				os.RemoveAll(mainDir)
			}
			if err := os.Rename(ingestDir, mainDir); err != nil {
				logger.WithField("from", ingestDir).WithField("to", mainDir).
					Errorf("finalize: failed to rename ingest dir: %v", err)
				continue
			}
			logger.WithField("from", ingestDir).WithField("to", mainDir).
				Debug("finalize: renamed ingest dir to main")
		}
	}
}

func readMigrationProps(migDir string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(migDir, "properties.mig"))
	if err != nil {
		return nil, err
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil, nil
	}
	return strings.Split(content, ","), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// reindexRevertedMarkerFile is the durable "boot-revert evidence" sentinel
// written by [restoreStagedDirsToOld] when it reverts a staged semantic-swap
// gen back to merged (0-weaviate-issues#220). It is the discriminator the
// flip-apply re-resolve hook ([Shard.reResolveStagedSwapsOnSchemaFlip] via
// [isRevertedMergedStagedGen]) requires before re-driving a merged-only gen:
// a gen that reached merged-only by FAILING mid-swap (Phase 2b) is on-disk
// identical in every other sentinel, but never went through the revert path,
// so it lacks this marker and is left untouched — its OLD backup survives.
const reindexRevertedMarkerFile = "reverted.mig"

// createFileDurableAt writes a sentinel file crash-durably: content fsync'd
// before close, parent dir fsync'd after, so the file survives a power cut
// once this returns nil. Package-level twin of
// [fileReindexTracker.createFileDurable] for the free-function finalize path
// (which has no tracker). Idempotent for sentinel use: an already-present
// marker (O_EXCL EEXIST) is treated as success.
func createFileDurableAt(dirPath, filename string, content []byte) error {
	path := filepath.Join(dirPath, filename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o777)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}
	if len(content) > 0 {
		if _, err := file.Write(content); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// migrationBucketSuffixes maps a migration dir name to its bucket naming scheme.
type migrationBucketSuffixes struct {
	sourceBucketName func(propName string) string
	ingestSuffix     string
	backupSuffix     string
}

func migrationSuffixes(migName string) *migrationBucketSuffixes {
	// Dir-name constants live in inverted_reindex_strategy_dir_names.go and
	// are referenced by each strategy's MigrationDirName() — keep finalize
	// in sync with the writer side by reusing the same constants here.
	//
	// Every migration dir name carries a `_<gen>` suffix appended by
	// [genSuffix]. The HasPrefix arms below match the strategy's prefix
	// regardless of the gen suffix; finalize callers compose the final
	// gen-suffixed sidecar dir name by appending `_<gen>` to the
	// ingest/backup suffix base.
	switch {
	case strings.HasPrefix(migName, MigrationDirSearchableMapToBlockmax):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__blockmax_ingest",
			backupSuffix:     "__blockmax_map",
		}
	case strings.HasPrefix(migName, MigrationDirFilterableRoaringsetRefresh):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__roaringset_ingest",
			backupSuffix:     "__roaringset_backup",
		}
	case strings.HasPrefix(migName, MigrationDirPrefixFilterableToRangeable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_rangeable" },
			ingestSuffix:     "__rangeable_ingest",
			backupSuffix:     "__rangeable_backup",
		}
	// Per-property dir names: "searchable_retokenize_<propName>"
	case strings.HasPrefix(migName, MigrationDirPrefixSearchableRetokenize):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__retokenize_ingest",
			backupSuffix:     "__retokenize_backup",
		}
	// Per-property dir names: "filterable_retokenize_<propName>"
	case strings.HasPrefix(migName, MigrationDirPrefixFilterableRetokenize):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__filt_retokenize_ingest",
			backupSuffix:     "__filt_retokenize_backup",
		}
	// Per-property dir names: "enable_filterable_<prop1>_<prop2>..." (see
	// EnableFilterableStrategy.MigrationDirName). The list of properties is
	// authoritative in properties.mig; the dir name is informational.
	case strings.HasPrefix(migName, MigrationDirPrefixEnableFilterable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__enable_filterable_ingest",
			backupSuffix:     "__enable_filterable_backup",
		}
	// Per-property dir names: "enable_searchable_<prop1>_<prop2>..." (see
	// EnableSearchableStrategy.MigrationDirName).
	case strings.HasPrefix(migName, MigrationDirPrefixEnableSearchable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__enable_searchable_ingest",
			backupSuffix:     "__enable_searchable_backup",
		}
	// Per-property dir names: "rebuild_searchable_<prop1>_<prop2>..." (see
	// RebuildSearchableStrategy.MigrationDirName).
	case strings.HasPrefix(migName, MigrationDirPrefixRebuildSearchable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__rebuild_searchable_ingest",
			backupSuffix:     "__rebuild_searchable_backup",
		}
	default:
		return nil
	}
}
