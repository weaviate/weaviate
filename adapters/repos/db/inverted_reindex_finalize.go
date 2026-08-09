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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
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
//     `markTidied`, so the ingest dir at gen M holds a complete dataset
//     under the target encoding while the canonical-name rename never
//     ran. What happens to that dataset — see
//     [swappedOrMergedPromotionDecision]. A generation carrying
//     `swapped.mig` is promoted unconditionally: the swap already
//     committed and there is no other complete copy left. Only a
//     generation without `swapped.mig` is weighed against the task that
//     produced it. When a generation is promoted, `swapped.mig` +
//     `tidied.mig` sentinels are written into gen-M's tracker dir (so
//     the namespace becomes self-consistent on disk and the same
//     finalize path runs) and gen M is promoted the same way.
//   - Remove every dir on disk (sidecars + tracker) with gen < effective
//     — these are pre-`effective` data, no longer referenced.
//   - Remove the tracker dir for `effective` itself, leaving a
//     `<dirName>.finalized.mig` marker in its place so a task callback
//     that arrives afterwards can tell "already done" from "never ran"
//     (see [migrationFinalizedMarkerPath]).
//   - If neither `T` nor `M` exists, do nothing — any earlier-stage
//     in-flight migration on disk is the recovery path's
//     responsibility ([DiscoverInFlightReindexTasks]).
//   - Generations with `gen > effective` are in-flight (next migration)
//     and left alone — recovery picks them up via their `payload.mig`.
//
// `class` is the collection schema this shard is being loaded with (the
// restored schema on a restore path) and `taskLiveness` resolves the
// task identity in a tracker's payload.mig against the distributed task
// list. Both are only consulted for a merged-without-tidied generation
// that does not carry `swapped.mig`. A nil lookup answers Unknown and a
// nil class confirms nothing, so nil does not mean "promote" — it means
// the decision falls back to Leave, or to Refuse for a task known to be
// dead. A swapped generation is promoted without consulting either.
//
// CRITICAL: This MUST be called BEFORE bucket loading, NEVER on live
// buckets. Renaming directories while buckets are open would corrupt
// the store. The deferred-finalize design relies on the in-memory swap
// (via DTM) marking tidied while the directory renames are deferred to
// the next startup when no buckets are loaded. See
// `docs/runtime-reindex.md` for the rationale.
func FinalizeCompletedMigrations(lsmPath string, class *models.Class,
	taskLiveness ReindexTaskLivenessLookup, logger logrus.FieldLogger,
) {
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
	type genInfo struct {
		dirName string
		gen     int
		tidied  bool
		merged  bool
		swapped bool
	}
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
		tidied := fileExists(filepath.Join(migrationsDir, name, "tidied.mig"))
		merged := fileExists(filepath.Join(migrationsDir, name, "merged.mig"))
		swapped := fileExists(filepath.Join(migrationsDir, name, "swapped.mig"))
		groups[namespace] = append(groups[namespace], genInfo{
			dirName: name,
			gen:     gen,
			tidied:  tidied,
			merged:  merged,
			swapped: swapped,
		})
	}

	for namespace, gens := range groups {
		// Find the highest tidied gen and the highest merged gen.
		// The "effective" promotion candidate is the larger of the two
		// — see the godoc on FinalizeCompletedMigrations for why a
		// merged-but-not-tidied gen is safe (and required) to promote.
		highestTidied := -1
		highestMerged := -1
		for _, g := range gens {
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
		// after markMerged but before markTidied. Whether its data may
		// be promoted depends on the task that produced it.
		if effective > highestTidied {
			for _, g := range gens {
				if g.gen != effective {
					continue
				}
				migDir := filepath.Join(migrationsDir, g.dirName)
				switch swappedOrMergedPromotionDecision(g.swapped, migDir, g.dirName, class, taskLiveness, logger) {
				case mergedPromotionPromote:
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
				case mergedPromotionLeave:
					effective = highestTidied
				case mergedPromotionRefuse:
					discardRefusedMergedGen(lsmPath, namespace, migDir, g.dirName, logger)
					effective = highestTidied
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
			migDir := filepath.Join(migrationsDir, g.dirName)
			switch {
			case g.gen == effective:
				finalizeMigrationDir(lsmPath, migDir, g.dirName, logger)
				// finalizeMigrationDir performs the ingest→canonical
				// rename + backup removal. We also remove the tracker
				// dir itself: its sentinels have done their job. The
				// marker takes its place so a task still running for
				// this generation can recognize its work as done.
				if err := os.RemoveAll(migDir); err != nil {
					logger.WithField("path", migDir).
						Warnf("reindex finalize: failed to remove finalized tracker dir: %v", err)
				}
				writeMigrationFinalizedMarker(lsmPath, g.dirName, logger)
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

		removeStaleFinalizedMarkers(migrationsDir, entries, namespace, effective, logger)
	}
}

// migrationFinalizedMarkerSuffix names the file [FinalizeCompletedMigrations]
// leaves behind when it removes a tracker dir it just promoted.
const migrationFinalizedMarkerSuffix = ".finalized.mig"

// migrationFinalizedMarkerPath is the marker recording that the given
// migration generation was promoted and its tracker removed.
//
// Removing the tracker is what makes the deferred rename visible to the
// next boot, but it also erases the only on-disk evidence that this
// generation ever existed. A task that is still running when startup
// promotes its generation calls back into the shard afterwards and finds
// nothing — indistinguishable from a shard that never ran the migration,
// which is a hard error ([ShardReindexTaskGeneric.enterDTMPhase]). The
// marker keeps those two apart so the task acks success instead of
// failing the whole cluster's migration on a shard whose data is correct.
//
// It is a file, not a directory, so the scans that enumerate migration
// state (all of which skip non-directories) do not see a finished
// migration as an in-flight one. The startup audit in
// [unexplainedEmptyRangeableProps] depends on that: a marker must not
// suppress its damage warning.
//
// Markers for superseded generations are swept by
// [removeStaleFinalizedMarkers], so at most one survives per namespace.
//
// A marker carrying my dirName does NOT prove that my generation was
// promoted, for two reasons:
//
//   - Generation numbers are reused. [nextMigrationGeneration] counts
//     directories, and a promoted generation leaves none behind, so the
//     next migration on the same namespace is handed the same number and
//     with it the same marker name.
//   - A marker whose namespace has no tracker dir never expires. The
//     sweep runs inside the per-namespace loop, and that loop is built
//     from tracker dirs only, so a lone marker survives every later boot.
//
// What makes reading the marker safe is that a tracker dir holding files
// overrides it — see [ShardReindexTaskGeneric.migrationAlreadyFinalized].
func migrationFinalizedMarkerPath(lsmPath, dirName string) string {
	return filepath.Join(lsmPath, ".migrations", dirName+migrationFinalizedMarkerSuffix)
}

// writeMigrationFinalizedMarker records that dirName's generation was
// promoted. Best-effort: a missing marker only costs the pre-existing
// failure mode it exists to prevent, so it must not abort finalize.
func writeMigrationFinalizedMarker(lsmPath, dirName string, logger logrus.FieldLogger) {
	path := migrationFinalizedMarkerPath(lsmPath, dirName)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		logger.WithField("path", path).
			Warnf("reindex finalize: failed to write finalized marker; a task still running for this generation may fail its swap callback: %v", err)
	}
}

// dirHoldsAnyFile reports whether a directory holds at least one regular
// file. Used to tell a migration tracker that carries state from an empty
// leftover dir, which some code paths create before finding there is
// nothing to do.
//
// A directory that cannot be read reports as empty. The error is returned
// alongside so callers for which "empty" is the unsafe answer can say so;
// a missing directory is empty and reports [os.ErrNotExist].
func dirHoldsAnyFile(dirPath string) (bool, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return true, nil
		}
	}
	return false, nil
}

// removeStaleFinalizedMarkers drops the markers of generations older than
// effective in one namespace. Their tasks cannot still be running: a
// newer generation of the same namespace has completed since.
func removeStaleFinalizedMarkers(migrationsDir string, entries []os.DirEntry,
	namespace string, effective int, logger logrus.FieldLogger,
) {
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), migrationFinalizedMarkerSuffix) {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), migrationFinalizedMarkerSuffix)
		ns, gen, ok := parseMigrationDirName(base)
		if !ok || ns != namespace || gen >= effective {
			continue
		}
		path := filepath.Join(migrationsDir, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logger.WithField("path", path).
				Warnf("reindex finalize: failed to remove stale finalized marker: %v", err)
		}
	}
}

// mergedPromotion is what [FinalizeCompletedMigrations] does with a
// generation that reached merged.mig but never reached tidied.mig.
type mergedPromotion int

const (
	// mergedPromotionPromote renames the ingest dir to canonical, the
	// deferred second half of a swap that did commit.
	mergedPromotionPromote mergedPromotion = iota
	// mergedPromotionLeave keeps every dir where it is. Either the task
	// is still running and owns them, or nothing on disk proves it is
	// not.
	mergedPromotionLeave
	// mergedPromotionRefuse discards the ingest dir and its tracker.
	mergedPromotionRefuse
)

// swappedOrMergedPromotionDecision decides what may happen to a
// generation that never reached tidied.mig.
//
// Everything [mergedPromotionDecision] weighs assumes the swap has not
// happened yet, so the canonical dir still holds the pre-migration data
// and declining to promote costs nothing. Once swapped.mig is on disk
// that assumption is gone: the swap renamed the canonical dir to
// backup_<gen> and flipped the in-memory pointer, so declining leaves no
// dir under the canonical name at all. Shard init then creates an empty
// one ([Shard.createPropertyValueIndex] does that for any property the
// schema says has an index), and the property serves zero rows until
// some later startup promotes the ingest dir.
//
// So swapped.mig makes the promotion the deferred second half of an
// operation that already committed, not a decision. The task's liveness
// and the schema only get a say before that point.
func swappedOrMergedPromotionDecision(swapped bool, migDir, dirName string, class *models.Class,
	taskLiveness ReindexTaskLivenessLookup, logger logrus.FieldLogger,
) mergedPromotion {
	if swapped {
		return mergedPromotionPromote
	}
	return mergedPromotionDecision(migDir, dirName, class, taskLiveness, logger)
}

// mergedPromotionDecision decides what may happen to a
// merged-but-unswapped, untidied generation, from the task identity in
// its payload.mig plus the collection schema.
//
// The dangerous case this exists for: a task that was cancelled or
// failed between markMerged and markTidied leaves an ingest dir that
// nothing will ever complete. Promoting it moves data the cluster
// decided not to migrate into the bucket queries read, and the schema
// disagrees with it — for change-tokenization that is live-wrong data
// on this replica only.
//
// Refusal is lossless, and that rests on the caller only asking about
// generations without swapped.mig: the swap has not renamed the old
// canonical dir away yet, so the property keeps serving its complete
// pre-migration data.
// The ingest dir stops being updated the moment the task dies, because
// the double-write mirror dies with it, so a later startup must never
// promote it either.
//
// Refusal is deliberately narrow. It fires only for a task that is known
// to be dead: a live task keeps its state, and so does a task whose
// liveness cannot be established. A tracker with no readable payload.mig
// keeps the historical behavior of promoting, since without the task
// identity nothing about it is known.
//
// When liveness is unknown the outcome is Leave, and the working dirs
// wait for a later startup or the orphan audit. Shards loaded eagerly
// during startup are in exactly that position: they initialize before
// [SetReindexAuditDeps] installs the task-list lookup, so refusal is
// reached only by shards loaded later, which in practice means
// lazily-loaded and multi-tenant ones.
func mergedPromotionDecision(migDir, dirName string, class *models.Class,
	taskLiveness ReindexTaskLivenessLookup, logger logrus.FieldLogger,
) mergedPromotion {
	logger = logger.WithField("migration", dirName)

	rec, ok := loadAuditRecord(migDir)
	if !ok {
		logger.Warn("reindex finalize: merged-but-untidied tracker has no readable payload.mig; " +
			"promoting it unverified. A tracker written by an older version, or one edited by hand, " +
			"has this shape")
		return mergedPromotionPromote
	}

	liveness := taskLiveness.Answer(rec.TaskID, rec.TaskVersion)
	agrees := mergedPromotionAgreesWithSchema(rec.Payload, class)

	logger = logger.WithField("task", rec.TaskID).
		WithField("task_liveness", liveness.String()).
		WithField("schema_agrees", agrees)

	switch {
	case liveness == ReindexTaskLivenessLive:
		logger.Info("reindex finalize: merged-but-untidied tracker belongs to a running task; " +
			"leaving its state to the reindex machinery")
		return mergedPromotionLeave
	case agrees:
		return mergedPromotionPromote
	case liveness == ReindexTaskLivenessDead:
		logger.Warnf("reindex finalize: refusing to promote merged-but-untidied migration — the %q task is "+
			"no longer running and the schema does not reflect it. The property keeps its pre-migration data "+
			"and the abandoned working dirs are discarded", rec.Payload.MigrationType)
		return mergedPromotionRefuse
	default:
		logger.Warnf("reindex finalize: the schema does not reflect the %q migration and its task's status is "+
			"unknown at this point in startup; leaving every dir in place for the next startup or the orphan audit",
			rec.Payload.MigrationType)
		return mergedPromotionLeave
	}
}

// mergedPromotionAgreesWithSchema reports whether `class` confirms the
// migration described by `payload` already completed for every property
// it targets. Content-equivalent rewrites (repair / rebuild / algorithm
// change) leave no schema trace and are always accepted, since their
// ingest bucket holds the same information as the canonical one it
// replaces; every other type needs its schema flag already flipped as
// proof. An unknown migration type is not confirmed, so a new strategy
// opts in here deliberately.
func mergedPromotionAgreesWithSchema(payload ReindexTaskPayload, class *models.Class) bool {
	var reflected func(prop *models.Property) bool
	switch payload.MigrationType {
	case ReindexTypeRepairFilterable, ReindexTypeRepairRangeable,
		ReindexTypeRebuildSearchable, ReindexTypeChangeAlgorithm:
		return true
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		reflected = func(prop *models.Property) bool {
			return payload.TargetTokenization != "" && prop.Tokenization == payload.TargetTokenization
		}
	case ReindexTypeEnableFilterable:
		reflected = inverted.HasFilterableIndex
	case ReindexTypeEnableSearchable:
		reflected = inverted.HasSearchableIndex
	case ReindexTypeEnableRangeable:
		reflected = inverted.HasRangeableIndex
	default:
		return false
	}

	if len(payload.Properties) == 0 {
		return false
	}
	for _, propName := range payload.Properties {
		prop := propertyByName(class, propName)
		if prop == nil || !reflected(prop) {
			return false
		}
	}
	return true
}

func propertyByName(class *models.Class, propName string) *models.Property {
	if class == nil {
		return nil
	}
	for _, p := range class.Properties {
		if p != nil && p.Name == propName {
			return p
		}
	}
	return nil
}

// discardRefusedMergedGen removes the working dirs a refused generation
// built (ingest and reindex) and then its tracker dir. Tracker last: a
// crash in between re-enters the same refusal on the next startup, and
// both steps are idempotent.
//
// Takes the backup guard, since a refused generation may be the one that
// left the property with no canonical dir at all. See
// [removeGenerationSidecars].
func discardRefusedMergedGen(lsmPath, namespace, migDir, dirName string, logger logrus.FieldLogger) {
	if err := removeGenerationSidecars(lsmPath, namespace, dirName, true, logger); err != nil {
		logger.WithField("path", migDir).
			Warnf("reindex finalize: cannot enumerate the refused migration's dirs; leaving them on disk for the orphan audit: %v", err)
		return
	}

	if err := os.RemoveAll(migDir); err != nil {
		logger.WithField("path", migDir).
			Warnf("reindex finalize: failed to remove refused merged tracker dir: %v", err)
	}
}

// removeGenerationSidecars removes the ingest, reindex and backup dirs
// of one generation of a migration. The generation is the `_<N>` tail of
// dirName; the suffix bases come from the strategy behind namespace.
//
// guardBackup keeps a backup dir that has no canonical dir beside it.
// That shape means a swap renamed the old main dir away and died before
// renaming ingest in, so the backup is the last copy of the property's
// data. Callers trimming a generation that finalized successfully pass
// false: their canonical dir is already in place.
//
// Returns an error only when the generation's dirs cannot be enumerated,
// in which case nothing was removed. Individual removal failures are
// logged and skipped, since the orphan audit picks them up later.
func removeGenerationSidecars(
	lsmPath, namespace, dirName string, guardBackup bool, logger logrus.FieldLogger,
) error {
	migDir := filepath.Join(lsmPath, ".migrations", dirName)
	suffixes := migrationSuffixes(dirName)
	if suffixes == nil {
		return fmt.Errorf("no known suffixes for migration dir %q", dirName)
	}
	_, gen, ok := parseMigrationDirName(dirName)
	if !ok {
		return fmt.Errorf("cannot parse generation out of migration dir %q", dirName)
	}
	props, err := readMigrationProps(migDir)
	if err != nil {
		return fmt.Errorf("read properties.mig: %w", err)
	}

	genTail := "_" + strconv.Itoa(gen)
	for _, propName := range props {
		main := suffixes.sourceBucketName(propName)
		for _, suff := range []string{suffixes.ingestSuffix, reindexSuffixForFinalize(namespace)} {
			if suff == "" {
				// An empty suffix would name the canonical dir itself.
				continue
			}
			removeDirIfPresent(filepath.Join(lsmPath, main+suff+genTail), logger)
		}

		backupDir := filepath.Join(lsmPath, main+suffixes.backupSuffix+genTail)
		if guardBackup && fileExists(backupDir) && !fileExists(filepath.Join(lsmPath, main)) {
			logger.WithField("property", propName).WithField("backup_dir", backupDir).
				Warn("reindex finalize: migration left a backup dir with no canonical dir beside it; " +
					"keeping it, it may be the only copy of this property's data")
			continue
		}
		removeDirIfPresent(backupDir, logger)
	}
	return nil
}

func removeDirIfPresent(path string, logger logrus.FieldLogger) {
	if !fileExists(path) {
		return
	}
	if err := os.RemoveAll(path); err != nil {
		logger.WithField("path", path).
			Warnf("reindex finalize: failed to remove refused migration dir: %v", err)
	}
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

// removeStaleSidecarsForGen removes the sidecar dirs belonging to an
// older, superseded generation of a finalized migration. That generation
// has a canonical dir already in place, so its backup dir needs no guard.
func removeStaleSidecarsForGen(lsmPath, namespace, dirName string, logger logrus.FieldLogger) {
	if err := removeGenerationSidecars(lsmPath, namespace, dirName, false, logger); err != nil {
		logger.WithField("path", filepath.Join(lsmPath, ".migrations", dirName)).
			Debugf("reindex finalize: stale-gen cleanup: cannot enumerate the generation's dirs; sidecars (if any) will be left as orphans: %v", err)
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
	suffixes := migrationSuffixes(migName)
	if suffixes == nil {
		return
	}
	_, gen, ok := parseMigrationDirName(migName)
	if !ok {
		// Defensive — every dir on disk should carry the gen suffix.
		return
	}
	genTail := "_" + strconv.Itoa(gen)

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
