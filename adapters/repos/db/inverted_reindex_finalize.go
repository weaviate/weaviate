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
	"sort"
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
//     swap on this node died AFTER `markMerged` but BEFORE
//     `markTidied`, so the ingest dir at gen M holds a complete dataset
//     under the TARGET encoding while the canonical-name rename never
//     ran. Promotion is conditional: `class` must agree the migration
//     completed (see [recoverMergedGen]). Only then are `swapped.mig` +
//     `tidied.mig` written into gen-M's tracker dir so the namespace
//     becomes self-consistent on disk and the same finalize path runs.
//   - Remove every dir on disk (sidecars + tracker) with gen < effective
//     — these are pre-`effective` data, no longer referenced.
//   - Remove the tracker dir for `effective` itself.
//   - If neither `T` nor `M` exists, do nothing — any earlier-stage
//     in-flight migration on disk is the recovery path's
//     responsibility ([DiscoverInFlightReindexTasks]).
//   - Generations with `gen > effective` are in-flight (next migration)
//     and left alone — recovery picks them up via their `payload.mig`.
//
// `class` is the collection schema this shard is being loaded with (the
// restored schema on a restore path). It is the proof source for the
// merged-without-tidied branch; pass nil only where no schema is
// available, which makes that branch refuse every promotion.
//
// Returns an error only when a promotion that MUST happen could not be
// completed — i.e. a property whose canonical dir is absent, where
// falling through would let initNonVector create an empty bucket in its
// place. Shard init propagates that; every other failure is logged and
// the tracker is kept on disk for the next startup to retry.
//
// CRITICAL: This MUST be called BEFORE bucket loading, NEVER on live
// buckets. Renaming directories while buckets are open would corrupt
// the store. The deferred-finalize design relies on the in-memory swap
// (via DTM) marking tidied while the directory renames are deferred to
// the next startup when no buckets are loaded. See
// `docs/runtime-reindex.md` for the rationale.
func FinalizeCompletedMigrations(lsmPath string, class *models.Class, logger logrus.FieldLogger) error {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		if os.IsNotExist(err) {
			// The normal "no migrations in progress" path.
			return nil
		}
		// EACCES, EIO, ENOTDIR and friends mean we cannot tell whether a
		// promotion is pending. Continuing would let initNonVector create
		// an empty bucket at a canonical name whose data is still sitting
		// in an un-promoted ingest dir, so fail instead and let the shard
		// retry once the filesystem problem is fixed.
		return fmt.Errorf("reading migrations dir %q: %w", migrationsDir, err)
	}

	// Group entries by namespace (prefix returned by parseMigrationDirName).
	// Within each namespace, find the highest tidied gen and any lower
	// gens to clean up. Higher (untidied) gens are deferred to recovery
	// EXCEPT when they have merged.mig — see the recovery path below.
	groups := map[string][]migrationGenInfo{}
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
		groups[namespace] = append(groups[namespace], migrationGenInfo{
			dirName: name,
			gen:     gen,
			tidied:  fileExists(filepath.Join(migrationsDir, name, "tidied.mig")),
			merged:  fileExists(filepath.Join(migrationsDir, name, "merged.mig")),
		})
	}

	for namespace, gens := range groups {
		if err := finalizeNamespace(lsmPath, namespace, gens, class, logger); err != nil {
			return err
		}
	}
	return nil
}

// migrationGenInfo is one generation's tracker dir as seen by
// [FinalizeCompletedMigrations]'s namespace grouping.
type migrationGenInfo struct {
	dirName string
	gen     int
	tidied  bool
	merged  bool
}

// finalizeNamespace runs [FinalizeCompletedMigrations]'s per-namespace
// algorithm: pick the effective promotion generation, promote it, and
// clean every older generation.
func finalizeNamespace(lsmPath, namespace string, gens []migrationGenInfo,
	class *models.Class, logger logrus.FieldLogger,
) error {
	// Find the highest tidied gen and the highest merged gen. The
	// "effective" promotion candidate is the larger of the two — see the
	// godoc on FinalizeCompletedMigrations.
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
		return nil
	}

	migrationsDir := filepath.Join(lsmPath, ".migrations")

	// promotable restricts which properties of the effective gen get
	// promoted. nil means "every property in properties.mig" — the
	// standard tidied path. The merged-recovery path narrows it to the
	// properties whose schema agrees the migration completed.
	var promotable map[string]bool

	if effective > highestTidied {
		for _, g := range gens {
			if g.gen != effective {
				continue
			}
			promotable = recoverMergedGen(lsmPath, namespace,
				filepath.Join(migrationsDir, g.dirName), g.dirName, class, logger)
			break
		}
		if len(promotable) == 0 {
			// Nothing may be promoted from the merged gen. Fall back to
			// the highest tidied gen (if any); the merged gen is now
			// `gen > effective` and is left alone by the loop below.
			effective = highestTidied
			promotable = nil
			if effective < 0 {
				return nil
			}
		}
	}

	// Finalize the effective promotion gen, then remove every gen <
	// effective (their data was superseded by this gen's complete
	// or recovered ingest dir).
	for _, g := range gens {
		migDir := filepath.Join(migrationsDir, g.dirName)
		switch {
		case g.gen == effective:
			keepTracker, err := finalizeMigrationDir(lsmPath, migDir, g.dirName, promotable, logger)
			if err != nil {
				return fmt.Errorf("finalizing migration %q: %w", g.dirName, err)
			}
			if keepTracker {
				// A property could not be promoted but its canonical dir
				// is intact, so the shard still serves correct (pre-
				// migration) data. Keeping the tracker lets the next
				// startup retry instead of orphaning the sidecars.
				logger.WithField("path", migDir).
					Errorf("reindex finalize: promotion incomplete; keeping tracker dir for the next startup to retry")
				continue
			}
			// finalizeMigrationDir performed the ingest→canonical
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
	return nil
}

// recoverMergedGen decides whether a merged-but-never-tidied generation
// may be promoted to the canonical bucket names, and returns the set of
// properties that may. An empty result means "promote nothing from this
// generation".
//
// A merged tracker only proves the reindex iteration finished and its
// segments were prepended into the ingest bucket. It does NOT prove the
// migration completed: `markMerged` is written by runtimePrepare during
// the PREP phase, long before any terminal task transition. A FAILED
// task therefore leaves the same on-disk shape as a node that died
// mid-swap, and nothing else on the startup path consumes merged
// residue (both the orphan audit and CleanStalePartialReindexState skip
// trackers carrying merged/tidied). Promoting unconditionally is how a
// failed reindex's leftovers replace a healthy index after a restore.
//
// The reliable source is the collection schema: for every migration type
// that changes what the bucket means, the schema flag flips only after
// the migration committed cluster-wide. So we promote a property iff its
// schema entry already describes the target state, and refuse otherwise.
// Refusal is lossless — the swap never renamed old-main→backup, so the
// canonical dir still holds the complete pre-migration data.
//
// A refused property's sidecar dirs are removed. They cannot be left for
// a later retry: once the task failed, disableCallbacks stopped mirroring
// live writes into the ingest bucket, so its contents go stale from that
// moment and must never be promoted later.
func recoverMergedGen(lsmPath, namespace, migDir, dirName string,
	class *models.Class, logger logrus.FieldLogger,
) map[string]bool {
	logger = logger.WithField("migration", dirName)

	props, err := readMigrationProps(migDir)
	if err != nil || len(props) == 0 {
		// Without the property list we can neither promote nor safely
		// discard. Leave every dir in place: the canonical buckets are
		// intact, so the shard serves correct pre-migration data.
		logger.Errorf("reindex finalize: merged-but-untidied tracker has no readable properties.mig; "+
			"refusing to promote and leaving its dirs in place for manual inspection: %v", err)
		return nil
	}

	rec, recOK := loadAuditRecord(migDir)
	promotable := map[string]bool{}
	var refused []string
	for _, propName := range props {
		if recOK && mergedPromotionAgreesWithSchema(rec.Payload, propName, class) {
			promotable[propName] = true
			continue
		}
		refused = append(refused, propName)
	}

	if len(refused) > 0 {
		reason := "payload.mig missing or unparseable"
		if recOK {
			reason = fmt.Sprintf("schema does not agree that the %q migration completed",
				rec.Payload.MigrationType)
		}
		logger.WithField("properties", refused).
			Errorf("reindex finalize: refusing to promote merged-but-untidied migration — %s; "+
				"the canonical buckets keep their pre-migration data and the leftover sidecars are discarded", reason)
		for _, propName := range refused {
			removeSidecarsForProp(lsmPath, namespace, dirName, propName, logger)
		}
	}

	if len(promotable) == 0 {
		// Tracker dir goes last: a crash between the sidecar removals and
		// here re-enters this same branch on the next startup, which
		// re-refuses and re-removes (both idempotent).
		if err := os.RemoveAll(migDir); err != nil {
			logger.WithField("path", migDir).
				Warnf("reindex finalize: failed to remove refused merged tracker dir: %v", err)
		}
		return nil
	}

	if err := writeRecoveryTidiedSentinels(migDir); err != nil {
		logger.Errorf("reindex finalize: failed to write recovery tidied sentinels; "+
			"promotion deferred to the next startup: %v", err)
		return nil
	}
	logger.WithField("properties", promotableSlice(promotable)).
		Info("reindex finalize: recovered untidied gen — runtime swap died post-merge and the schema agrees, completing finalize from disk state")
	return promotable
}

// mergedPromotionAgreesWithSchema reports whether `class` confirms the
// migration described by `payload` already completed for `propName`.
// Content-equivalent rewrites (repair / rebuild / algorithm change) are
// always safe since the ingest bucket holds the same information as the
// canonical one it replaces; every other type needs its schema flag
// already flipped as proof. An unknown migration type is refused, so a
// new strategy must opt in here deliberately.
func mergedPromotionAgreesWithSchema(payload ReindexTaskPayload, propName string, class *models.Class) bool {
	prop := propertyByName(class, propName)
	switch payload.MigrationType {
	case ReindexTypeRepairFilterable, ReindexTypeRepairRangeable,
		ReindexTypeRebuildSearchable, ReindexTypeChangeAlgorithm:
		return true
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		return prop != nil && payload.TargetTokenization != "" &&
			prop.Tokenization == payload.TargetTokenization
	case ReindexTypeEnableFilterable:
		return prop != nil && inverted.HasFilterableIndex(prop)
	case ReindexTypeEnableSearchable:
		return prop != nil && inverted.HasSearchableIndex(prop)
	case ReindexTypeEnableRangeable:
		return prop != nil && inverted.HasRangeableIndex(prop)
	default:
		return false
	}
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

// promotableSlice flattens a promotable-property set into a sorted
// []string for stable structured-log output.
func promotableSlice(promotable map[string]bool) []string {
	out := make([]string, 0, len(promotable))
	for p := range promotable {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
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
	suffixes := migrationSuffixes(dirName)
	if suffixes == nil {
		return
	}
	props, err := readMigrationProps(migDir)
	if err != nil {
		logger.WithField("path", migDir).
			Debugf("reindex finalize: stale-gen cleanup: properties.mig missing/unreadable; sidecars (if any) will be left as orphans: %v", err)
		return
	}
	// The gen suffix is implicit in `dirName`'s trailing `_<N>`; the
	// strategy's suffix methods compute IngestSuffix/etc. as
	// `<base>_<N>`. We don't have the strategy instance here, so emulate
	// by appending the same gen to each suffix base.
	_, gen, ok := parseMigrationDirName(dirName)
	if !ok {
		return
	}
	genTail := "_" + strconv.Itoa(gen)
	for _, propName := range props {
		removeSidecarDirs(lsmPath, namespace, suffixes, propName, genTail, logger)
	}
}

// removeSidecarsForProp removes the reindex/ingest/backup sidecar dirs of
// a single property in one generation's migration. Used by the
// merged-recovery refusal path, where some properties of a multi-property
// migration are promoted and the rest are discarded.
func removeSidecarsForProp(lsmPath, namespace, dirName, propName string, logger logrus.FieldLogger) {
	suffixes := migrationSuffixes(dirName)
	if suffixes == nil {
		return
	}
	_, gen, ok := parseMigrationDirName(dirName)
	if !ok {
		return
	}
	removeSidecarDirs(lsmPath, namespace, suffixes, propName, "_"+strconv.Itoa(gen), logger)
}

func removeSidecarDirs(lsmPath, namespace string, suffixes *migrationBucketSuffixes,
	propName, genTail string, logger logrus.FieldLogger,
) {
	main := suffixes.sourceBucketName(propName)
	for _, suff := range []string{suffixes.ingestSuffix, suffixes.backupSuffix, reindexSuffixForFinalize(namespace)} {
		removeDirIfPresent(filepath.Join(lsmPath, main+suff+genTail), logger)
	}
}

func removeDirIfPresent(path string, logger logrus.FieldLogger) {
	if !fileExists(path) {
		return
	}
	if err := os.RemoveAll(path); err != nil {
		logger.WithField("path", path).
			Warnf("reindex finalize: failed to remove dir: %v", err)
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

// finalizeMigrationDir performs the deferred ingest→canonical rename and
// backup removal for one generation's migration.
//
// `onlyProps` restricts the work to a subset of `properties.mig`; nil
// means every property. The merged-recovery path passes a subset when
// some properties of a multi-property migration were refused.
//
// Returns keepTracker=true when a property could not be finalized but its
// canonical dir is intact — the shard still serves correct data, so the
// caller keeps the tracker for the next startup instead of orphaning the
// sidecars. Returns an error when a property's canonical dir is ABSENT
// and the promotion that would create it failed: falling through there
// lets initNonVector create an empty bucket at the canonical name, which
// is silent data loss.
func finalizeMigrationDir(lsmPath, migDir, migName string, onlyProps map[string]bool,
	logger logrus.FieldLogger,
) (keepTracker bool, err error) {
	logger = logger.WithField("migration", migName)

	// Only finalize if both swapped and tidied sentinels exist.
	if !fileExists(filepath.Join(migDir, "swapped.mig")) || !fileExists(filepath.Join(migDir, "tidied.mig")) {
		logger.Errorf("finalize: tracker is missing swapped.mig or tidied.mig; nothing promoted")
		return true, nil
	}

	// Determine bucket naming from migration dir name. The migration dir
	// name carries a `_<gen>` suffix (e.g. `searchable_retokenize_text_2`);
	// the strategy's IngestSuffix / BackupSuffix methods on the writer
	// side appended the same gen to the suffix base. Reproduce that here
	// to find the matching on-disk sidecar dirs.
	suffixes := migrationSuffixes(migName)
	if suffixes == nil {
		logger.Errorf("finalize: unknown migration strategy prefix; nothing promoted")
		return true, nil
	}
	_, gen, ok := parseMigrationDirName(migName)
	if !ok {
		// Defensive — every dir on disk should carry the gen suffix.
		logger.Errorf("finalize: tracker dir name carries no generation suffix; nothing promoted")
		return true, nil
	}
	genTail := "_" + strconv.Itoa(gen)

	// Read properties from the migration. Without them we cannot name the
	// sidecar dirs — but we can still tell whether any of them is a
	// pending promotion whose canonical dir is missing, which is the one
	// case that must fail the shard rather than limp on.
	props, err := readMigrationProps(migDir)
	if err != nil || len(props) == 0 {
		if orphan := pendingPromotionWithoutCanonical(lsmPath, suffixes, genTail); orphan != "" {
			return true, fmt.Errorf(
				"tracker %q has no readable properties.mig (%v) while ingest dir %q awaits promotion "+
					"and its canonical dir does not exist; refusing to start the shard with an empty bucket",
				migName, err, orphan)
		}
		logger.Errorf("finalize: properties.mig is missing or unreadable; nothing promoted: %v", err)
		return true, nil
	}

	for _, propName := range props {
		if onlyProps != nil && !onlyProps[propName] {
			continue
		}
		mainName := suffixes.sourceBucketName(propName)
		ingestDir := filepath.Join(lsmPath, mainName+suffixes.ingestSuffix+genTail)
		backupDir := filepath.Join(lsmPath, mainName+suffixes.backupSuffix+genTail)
		mainDir := filepath.Join(lsmPath, mainName)

		// The backup dir holds the pre-swap data. It is removed LAST, only
		// once the canonical dir is in place, so a torn state can never end
		// with the backup deleted and nothing serving the property.
		//
		// Neither an ingest dir to promote nor a canonical dir is the
		// superseded-namespace shape, and it is routine: a later migration
		// on the same property resolved the same canonical name, so its
		// runtimeSwap Phase 2b renamed THIS namespace's ingest dir away to
		// its own backup name. The live data belongs to that namespace,
		// which promotes it in this same finalize pass. This namespace has
		// nothing left to do.
		//
		// Do not fail the shard here. Nothing at this point can tell that
		// case apart from a genuinely torn one, and taking a node down over
		// data that is intact is the worse error. What matters is skipping
		// the backup removal below, so a pre-swap copy survives either way.
		if !fileExists(ingestDir) && !fileExists(mainDir) {
			if fileExists(backupDir) {
				logger.WithField("property", propName).WithField("backup_dir", backupDir).
					Warn("finalize: nothing to promote and no canonical dir; leaving the backup dir in place")
			} else {
				logger.WithField("property", propName).
					Debug("finalize: nothing to promote and no canonical dir; another migration on this property owns it")
			}
			continue
		}

		if fileExists(ingestDir) {
			// Remove a stale canonical dir if one exists (shouldn't
			// normally, but be safe).
			if fileExists(mainDir) {
				if rmErr := os.RemoveAll(mainDir); rmErr != nil {
					logger.WithField("dir", mainDir).
						Errorf("finalize: failed to remove stale canonical dir before promotion: %v", rmErr)
					keepTracker = true
					continue
				}
			}
			if rnErr := os.Rename(ingestDir, mainDir); rnErr != nil {
				// The canonical dir is gone either way at this point (it was
				// removed above, or never existed), so the shard would come up
				// with an empty bucket. Fail loudly instead.
				return true, fmt.Errorf("renaming ingest dir %q to canonical %q for property %q: %w",
					ingestDir, mainDir, propName, rnErr)
			}
			logger.WithField("from", ingestDir).WithField("to", mainDir).
				Debug("finalize: renamed ingest dir to main")
		}

		// The canonical dir is in place, so the backup is redundant now. A
		// failure here only leaves a stale dir behind.
		if fileExists(backupDir) {
			if rmErr := os.RemoveAll(backupDir); rmErr != nil {
				logger.WithField("dir", backupDir).
					Errorf("finalize: failed to remove backup dir: %v", rmErr)
				keepTracker = true
			} else {
				logger.WithField("dir", backupDir).Debug("finalize: removed backup dir")
			}
		}
	}
	return keepTracker, nil
}

// pendingPromotionWithoutCanonical returns the path of the first ingest
// sidecar dir at `genTail` whose canonical dir is missing, or "" when
// there is none. It recovers the property set from the on-disk dir names
// so [finalizeMigrationDir] can still classify a tracker whose
// properties.mig is unreadable.
func pendingPromotionWithoutCanonical(lsmPath string, suffixes *migrationBucketSuffixes, genTail string) string {
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return ""
	}
	tail := suffixes.ingestSuffix + genTail
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasSuffix(entry.Name(), tail) {
			continue
		}
		mainName := strings.TrimSuffix(entry.Name(), tail)
		if mainName == "" {
			continue
		}
		if !fileExists(filepath.Join(lsmPath, mainName)) {
			return filepath.Join(lsmPath, entry.Name())
		}
	}
	return ""
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
