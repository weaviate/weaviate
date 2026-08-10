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
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Migration directory names live under <shard>/lsm/.migrations/<name>/ and
// uniquely identify a per-strategy in-progress migration on a shard.
//
// Three concerns need to agree on these names:
//  1. Each strategy's MigrationDirName() return value (the writer side).
//  2. The startup finalizer (migrationSuffixes in inverted_reindex_finalize.go),
//     which scans .migrations/ before buckets are loaded and decides which
//     directory rename / cleanup recipe to apply.
//  3. Debug endpoints (handlers_debug_bmw_aux.go) that touch the migration
//     directory directly.
//
// To prevent silent drift between writer / finalizer / debug, define each
// name exactly once here and reference the constant from all three places.
//
// Some strategies pin a single directory (e.g. searchable_map_to_blockmax),
// others suffix per-property names onto a common prefix (e.g.
// enable_filterable_<prop1>_<prop2>) — for those, the constant is the
// prefix and callers append.
const (
	// MigrationDirSearchableMapToBlockmax is the directory name for the
	// MapCollection → Inverted (blockmax WAND) migration of searchable
	// properties.
	MigrationDirSearchableMapToBlockmax = "searchable_map_to_blockmax"

	// MigrationDirFilterableRoaringsetRefresh is the directory name for the
	// same-strategy rebuild of an existing filterable (RoaringSet) index.
	MigrationDirFilterableRoaringsetRefresh = "filterable_roaringset_refresh"

	// MigrationDirPrefixFilterableToRangeable is the directory-name prefix
	// for the filterable → rangeable migration. The actual directory is
	// either this prefix on its own (no specific properties) or this prefix
	// + "_<prop1>_<prop2>...". Use as both equality check (no propnames) and
	// HasPrefix check.
	MigrationDirPrefixFilterableToRangeable = "filterable_to_rangeable"

	// MigrationDirPrefixSearchableRetokenize is the directory-name prefix
	// for the per-property retokenize migration on the searchable index.
	// Actual dir: "<prefix>_<propName>".
	MigrationDirPrefixSearchableRetokenize = "searchable_retokenize"

	// MigrationDirPrefixFilterableRetokenize is the directory-name prefix
	// for the per-property retokenize migration on the filterable index.
	// Actual dir: "<prefix>_<propName>".
	MigrationDirPrefixFilterableRetokenize = "filterable_retokenize"

	// MigrationDirPrefixEnableFilterable is the directory-name prefix for
	// the enable-filterable migration. The actual directory is either this
	// prefix on its own (no specific properties) or this prefix +
	// "_<prop1>_<prop2>...".
	MigrationDirPrefixEnableFilterable = "enable_filterable"

	// MigrationDirPrefixEnableSearchable is the directory-name prefix for
	// the enable-searchable migration. The actual directory is either this
	// prefix on its own (no specific properties) or this prefix +
	// "_<prop1>_<prop2>...".
	MigrationDirPrefixEnableSearchable = "enable_searchable"

	// MigrationDirPrefixRebuildSearchable is the directory-name prefix for
	// the per-property rebuild-searchable migration (rebuild a BlockMax
	// searchable bucket from the objects store). Actual dir:
	// "<prefix>_<prop1>_<prop2>...".
	MigrationDirPrefixRebuildSearchable = "rebuild_searchable"
)

// migrationDirWithProps assembles a migration directory name from a
// prefix and an optional set of property names. Empty propNames returns
// the prefix on its own; otherwise the prefix is joined with the
// property names by underscores. Three strategies (enable-filterable,
// enable-searchable, filterable-to-rangeable) share this naming pattern.
//
// Property names are sorted before joining so that the directory name
// is a function of the *set* of properties, not the caller's slice
// order. This keeps restart-recovery deterministic: a task built from
// payload.Properties=["b","a"] and one built from ["a","b"] both
// resolve to the same on-disk directory.
func migrationDirWithProps(prefix string, propNames []string) string {
	if len(propNames) == 0 {
		return prefix
	}
	sorted := make([]string, len(propNames))
	copy(sorted, propNames)
	sort.Strings(sorted)
	return prefix + "_" + strings.Join(sorted, "_")
}

// genSuffix returns the per-migration generation suffix, e.g. "_2".
// Every concrete strategy's MigrationDirName / ReindexSuffix / IngestSuffix /
// BackupSuffix appends this so back-to-back in-process migrations on the
// same (prop, indexType) tuple don't collide on dir paths. Generation is
// computed per-node at task start by [nextMigrationGeneration]; the
// previous live main bucket lives at `…_ingest_<N-1>` (the in-memory
// pointer was already swapped to it; on-disk rename is deferred to the
// next-restart finalize), and the new migration writes to `…_ingest_<N>`.
//
// Generation 0 is reserved for the canonical (post-finalize) bucket at
// `property_<prop>_<index>`, which has no suffix. Live migrations always
// use generation ≥ 1.
func genSuffix(generation int) string {
	return "_" + strconv.Itoa(generation)
}

// parseMigrationDirName splits a migration dir name (e.g.
// "searchable_retokenize_text_2", "enable_filterable_p1_p2_3",
// "searchable_map_to_blockmax_1") into its (prefix, generation) parts.
// The "prefix" returned is everything up to and excluding the trailing
// "_<N>" — for per-property strategies it includes the property name(s).
//
// Returns ok=false if the input does not end with "_<positive-int>". The
// finalize / recovery paths use this to enumerate generations on disk and
// pick the right strategy instance + gen.
func parseMigrationDirName(name string) (prefix string, generation int, ok bool) {
	idx := strings.LastIndex(name, "_")
	if idx <= 0 || idx == len(name)-1 {
		return "", 0, false
	}
	gen, err := strconv.Atoi(name[idx+1:])
	if err != nil || gen < 1 {
		return "", 0, false
	}
	return name[:idx], gen, true
}

// classLevelMigrationDirForIndexType returns the class-level strategy's
// migration dir prefix for an indexType. Excluded from deletion in
// [migrationDirPrefixesForIndexType], but its completed gens must still feed
// the preserve set in CleanStalePartialReindexState: their sidecars are live.
func classLevelMigrationDirForIndexType(indexType string) (string, bool) {
	switch indexType {
	case "filterable":
		return MigrationDirFilterableRoaringsetRefresh, true
	case "searchable":
		return MigrationDirSearchableMapToBlockmax, true
	default:
		return "", false
	}
}

// migrationDirPrefixesForIndexType returns the per-property migration
// strategy prefixes whose tracker dirs — if marked tidied on disk — would
// lie after a (property, indexType) bucket has been removed. Called from
// updatePropertyBuckets after a DELETE so that a subsequent re-enable
// starts from a clean slate instead of short-circuiting on a stale
// "previous run completed" sentinel.
//
// indexType is the canonical inverted-index discriminator:
// "filterable", "searchable", or "rangeable".
//
// Class-level migration dirs (searchable_map_to_blockmax,
// filterable_roaringset_refresh) are deliberately omitted — they
// aggregate state across every property of the class and per-property
// progress lives inside the dir, not as the dir's own existence.
// Wholesale-deleting them on a single property's DELETE would corrupt
// the class-level migration; their per-property entries are pruned by
// the strategy's own bookkeeping. [migrationDirScope.preserving] adds
// them back for the preserve set, which must span them.
func migrationDirPrefixesForIndexType(indexType string) []string {
	switch indexType {
	case "filterable":
		return []string{
			MigrationDirPrefixEnableFilterable,
			MigrationDirPrefixFilterableRetokenize,
			MigrationDirPrefixFilterableToRangeable,
		}
	case "searchable":
		return []string{
			MigrationDirPrefixEnableSearchable,
			MigrationDirPrefixRebuildSearchable,
			MigrationDirPrefixSearchableRetokenize,
		}
	case "rangeable":
		return []string{
			MigrationDirPrefixFilterableToRangeable,
		}
	}
	return nil
}

// migrationDirScope names the migration tracker dirs one (property, index
// type) cleanup owns on one shard, and decides which dir on disk is one of
// them.
//
// The dir name alone cannot decide that. [migrationDirWithProps] joins a
// task's whole property list into the name, so "enable_filterable_a_b_1" is
// the tracker of a two-property task on "a" and "b" and the tracker of a
// single property named "a_b", written identically. The task recorded its
// property list in payload.mig before it wrote anything else, so that list
// decides. The name is only read when there is no readable payload, and then
// it answers for the single-property shape alone: a multi-property tracker
// with no payload is left to the next-restart finalizer rather than matched
// on a guess that could delete another property's state.
//
// One ambiguity survives in that fallback, from before payload.mig existed: a
// sweep of "cat" matches "enable_filterable_cat_2", which is either this
// property's generation 2 or the generation-less tracker of property "cat_2".
// It is matched, as it was before generations existed.
type migrationDirScope struct {
	lsmPath  string
	dirs     *dirNamesCache
	propName string
	// prefixes are the per-property strategy prefixes this cleanup deletes.
	prefixes []string
	// classDirs are whole tracker dir names matched as they are. Only the
	// preserve set carries them; see [migrationDirScope.preserving].
	classDirs []string
}

// migrationDirsOf returns the tracker dirs a (propName, indexType) cleanup
// deletes on the shard at lsmPath. A nil cache reads the filesystem every time.
func migrationDirsOf(lsmPath string, dirs *dirNamesCache, propName, indexType string) migrationDirScope {
	return migrationDirScope{
		lsmPath:  lsmPath,
		dirs:     dirs,
		propName: propName,
		prefixes: migrationDirPrefixesForIndexType(indexType),
	}
}

// classLevelMigrationDirsOf returns the scope of a single class-level tracker
// dir, which every property of the collection shares.
func classLevelMigrationDirsOf(lsmPath, classDir string) migrationDirScope {
	return migrationDirScope{lsmPath: lsmPath, classDirs: []string{classDir}}
}

// preserving widens a cleanup's scope to the dirs whose completed generations
// it must not remove. That is MORE than it deletes: a class-level migration's
// tracker is excluded from deletion, but a completed one owns live sidecars of
// every property, and wiping those is #10675-shape data loss.
//
// The gate that decides whether to load a cold shard and the sweep that runs on
// the loaded one both build their preserve set here, so the two cannot drift
// into a gate that skips shards the sweep would have cleaned.
func (s migrationDirScope) preserving(indexType string) migrationDirScope {
	if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
		// Cloned because the receiver is a value: appending into the caller's
		// backing array would widen its scope too if it ever had spare capacity.
		s.classDirs = append(slices.Clone(s.classDirs), classDir)
	}
	return s
}

// matches reports whether the tracker dir called name is in this scope. See
// [migrationDirScope] for why the payload decides and the name only fills in.
func (s migrationDirScope) matches(name string) bool {
	base, _, ok := parseMigrationDirName(name)
	if !ok {
		// A dir with no generation suffix predates [genSuffix] and carries the
		// prefix as its whole name.
		base = name
	}
	for _, classDir := range s.classDirs {
		if base == classDir {
			return true
		}
	}
	if !s.hasStrategyPrefix(base) {
		// Nothing this cleanup owns is named like this, so its payload is not
		// worth reading. A cold shard's .migrations dir is walked once per
		// (property, index type), and most of what is in it belongs to other
		// tuples.
		return false
	}
	if props, ok := s.taskProperties(name); ok {
		if !slices.Contains(props, s.propName) {
			return false
		}
		for _, prefix := range s.prefixes {
			if base == migrationDirWithProps(prefix, props) {
				return true
			}
		}
		return false
	}
	for _, prefix := range s.prefixes {
		if base == migrationDirWithProps(prefix, []string{s.propName}) {
			return true
		}
	}
	return false
}

// hasStrategyPrefix reports whether a tracker dir's base could belong to one of
// this scope's strategies, whatever properties it names.
func (s migrationDirScope) hasStrategyPrefix(base string) bool {
	for _, prefix := range s.prefixes {
		if strings.HasPrefix(base, prefix+"_") {
			return true
		}
	}
	return false
}

// taskProperties returns the property list the task recorded in its tracker
// dir. Reports ok=false when no payload is readable there, and for a
// class-level task, which records no property list.
func (s migrationDirScope) taskProperties(name string) ([]string, bool) {
	props, ok := readRecoveryPropertyNames(filepath.Join(s.lsmPath, ".migrations", name))
	if !ok || len(props) == 0 {
		return nil, false
	}
	return props, true
}
