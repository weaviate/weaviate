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

// migrationDirsForPropertyIndex returns the per-property migration
// directory names that — if marked tidied on disk — would lie after the
// given (propName, indexType) bucket has been removed. Called from
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
// the strategy's own bookkeeping.
func migrationDirsForPropertyIndex(propName, indexType string) []string {
	switch indexType {
	case "filterable":
		return []string{
			migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{propName}),
			MigrationDirPrefixFilterableRetokenize + "_" + propName,
			migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, []string{propName}),
		}
	case "searchable":
		return []string{
			migrationDirWithProps(MigrationDirPrefixEnableSearchable, []string{propName}),
			MigrationDirPrefixSearchableRetokenize + "_" + propName,
		}
	case "rangeable":
		return []string{
			migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, []string{propName}),
		}
	}
	return nil
}
