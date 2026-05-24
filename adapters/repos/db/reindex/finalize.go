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

package reindex

import (
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
		prefix, gen, ok := ParseMigrationDirName(entry.Name())
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

// CompletedMigrationGens returns the set of generation numbers whose
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
// [MigrationDirsForPropertyIndex] for the (propName, indexType) tuple.
func CompletedMigrationGens(lsmPath string, prefixes []string) map[int]bool {
	out := map[int]bool{}
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return out
	}
	prefixSet := map[string]bool{}
	for _, p := range prefixes {
		prefixSet[p] = true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, gen, ok := ParseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		if !prefixSet[base] {
			continue
		}
		dirPath := filepath.Join(migrationsDir, entry.Name())
		if FileExistsInDir(dirPath, "tidied.mig") || FileExistsInDir(dirPath, "merged.mig") {
			out[gen] = true
		}
	}
	return out
}

// FileExistsInDir is a small helper for [CompletedMigrationGens]; returns
// true iff the named file is present in dirPath as a regular file.
func FileExistsInDir(dirPath, fileName string) bool {
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
// by [ParseMigrationDirName]):
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

	// Group entries by namespace (prefix returned by ParseMigrationDirName).
	// Within each namespace, find the highest tidied gen and any lower
	// gens to clean up. Higher (untidied) gens are deferred to recovery
	// EXCEPT when they have merged.mig — see the recovery path below.
	type genInfo struct {
		dirName string
		gen     int
		tidied  bool
		merged  bool
	}
	groups := map[string][]genInfo{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		namespace, gen, ok := ParseMigrationDirName(name)
		if !ok {
			// Entry doesn't follow the `<prefix>_<N>` convention. Skip —
			// this branch never produces such entries; defensive.
			continue
		}
		tidied := fileExists(filepath.Join(migrationsDir, name, "tidied.mig"))
		merged := fileExists(filepath.Join(migrationsDir, name, "merged.mig"))
		groups[namespace] = append(groups[namespace], genInfo{
			dirName: name,
			gen:     gen,
			tidied:  tidied,
			merged:  merged,
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
	_, gen, ok := ParseMigrationDirName(dirName)
	if !ok {
		return
	}
	genTail := "_" + strconv.Itoa(gen)
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
	suffixes := migrationSuffixes(migName)
	if suffixes == nil {
		return
	}
	_, gen, ok := ParseMigrationDirName(migName)
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
