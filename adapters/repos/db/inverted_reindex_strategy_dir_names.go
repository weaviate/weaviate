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
	"os"
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
// strategy prefixes for a "filterable"/"searchable"/"rangeable" indexType,
// whose tracker dirs would lie (report "previous run completed") after the
// bucket has been removed on a property DELETE.
//
// Class-level migration dirs are deliberately omitted: they aggregate state
// across every property, so deleting one on a single property's DELETE would
// corrupt migrations for the rest of the class.
// [migrationDirScope.preserving] adds them back for the preserve set.
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
// The dir name alone is ambiguous — "enable_filterable_a_b_1" is both a
// two-property tracker for "a" and "b" and a single-property tracker for
// "a_b" — so payload.mig, written before anything else, decides. With no
// readable payload, the two directions guess differently because a wrong
// guess costs differently:
//
//   - Deletion (the plain scope) answers only the single-property shape;
//     guessing wider would remove another property's tracker. The refusal
//     leaves a payload-less multi-property tracker behind while its sidecars
//     — whose deletion is not payload-gated — are removed, and nothing
//     reclaims it: the orphan audit skips a tracker with no started.mig.
//   - Preservation ([migrationDirScope.preserving]) also accepts a dir whose
//     property list carries this property as a whole "_"-delimited token,
//     because refusing to guess lets sidecar deletion — which is not
//     payload-gated — remove the live bucket the in-memory pointer is on.
//
// The preserve direction therefore over-matches: e.g. "cat" also keeps
// unrelated "cat_x", and "b_a" keeps "a" — the name can't tell them apart.
// This is the cheaper failure: an over-kept dir costs a recoverable
// "rename: file exists" on re-enable, while an under-kept one deletes live
// data.
type migrationDirScope struct {
	lsmPath  string
	dirs     *dirNamesCache
	propName string
	// prefixes are the per-property strategy prefixes this cleanup deletes.
	prefixes []string
	// classDirs are whole tracker dir names matched as they are. Only the
	// preserve set carries them; see [migrationDirScope.preserving].
	classDirs []string
	// preserve widens the no-payload fallback in [migrationDirScope.matches];
	// set by [migrationDirScope.preserving].
	preserve bool
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

// preserving widens the scope, both to keep live data out of the sweep's
// reach (else #10675-shape data loss): the class-level tracker for indexType
// joins it (a completed one owns live sidecars of every property), and a
// tracker with no readable payload matches on its name alone; see
// [migrationDirScope]. Used identically by the unloaded-shard gate and the
// sweep so the two can't drift apart.
func (s migrationDirScope) preserving(indexType string) migrationDirScope {
	s.preserve = true
	if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
		// Cloned so two preserving() results can't share a backing array.
		// Unreachable today (both constructors leave classDirs empty or are
		// never widened); kept for the constructor that changes that.
		s.classDirs = append(slices.Clone(s.classDirs), classDir)
	}
	return s
}

// matches reports whether the tracker dir called name is in this scope. See
// [migrationDirScope] for why the payload decides and the name only fills in.
//
// Retokenize strategies name their dir with a bare property name; that's safe
// only because [ReindexProvider.createReindexTasks] rejects such a payload
// unless it carries exactly one property.
func (s migrationDirScope) matches(name string) bool {
	matched, _ := s.match(name)
	return matched
}

// match additionally reports a payload that exists but could not be read or
// parsed. Deletion ([migrationDirScope.matches]) ignores that and keeps the
// no-payload fallback — deleting on a guess could remove another property's
// tracker — while the unloaded-shard gate and the recovery probe
// ([hasUntidiedTracker]) fail open on it, since answering from the narrowed
// fallback could report as clean (or recovered) state that the payload, once
// readable again, says they own.
//
// An intact payload naming this property still requires the exact sorted-name
// reconstruction, so a dir whose name lists its properties unsorted would be
// preserved with a missing or corrupt payload (name-token fallback) but not
// with an intact one — unreachable from real writers, which derive the name
// and the payload from the same sorted list.
func (s migrationDirScope) match(name string) (matched, unreadablePayload bool) {
	base, _, ok := parseMigrationDirName(name)
	if !ok {
		// A dir with no generation suffix predates [genSuffix] and carries the
		// prefix as its whole name.
		base = name
	}
	for _, classDir := range s.classDirs {
		if base == classDir {
			return true, false
		}
	}
	if !s.hasStrategyPrefix(base) {
		// Not this cleanup's dir; skip reading its payload.
		return false, false
	}
	props, ok, unreadable := s.taskProperties(name)
	if ok {
		if !slices.Contains(props, s.propName) {
			return false, false
		}
		for _, prefix := range s.prefixes {
			if base == migrationDirWithProps(prefix, props) {
				return true, false
			}
		}
		return false, false
	}
	for _, prefix := range s.prefixes {
		if base == migrationDirWithProps(prefix, []string{s.propName}) {
			return true, unreadable
		}
		if s.preserve && namesPropertyToken(base, prefix, s.propName) {
			return true, unreadable
		}
	}
	return false, unreadable
}

// namesPropertyToken reports whether base is prefix + "_" + a property list
// that carries propName as one whole "_"-delimited token, e.g.
// "enable_filterable_a_b" for propName "a".
//
// A whole token is as precise as the name gets: a single property named "a_b"
// produces the identical dir name, so this also reports true for a property
// whose own name merely extends propName across "_". See [migrationDirScope]
// for why over-matching is the safe direction here.
//
// The single-token shape (props == propName) is not checked here: the exact
// single-property equality in [migrationDirScope.match] answers it before
// this is consulted.
func namesPropertyToken(base, prefix, propName string) bool {
	props, ok := strings.CutPrefix(base, prefix+"_")
	if !ok {
		return false
	}
	return strings.HasPrefix(props, propName+"_") ||
		strings.HasSuffix(props, "_"+propName) ||
		strings.Contains(props, "_"+propName+"_")
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
// dir. ok=false with unreadable=false means the task recorded nothing (no
// payload file, or one naming no property); unreadable=true means a payload
// is there but its content couldn't be obtained, so "recorded nothing" is not
// a safe conclusion.
func (s migrationDirScope) taskProperties(name string) (props []string, ok, unreadable bool) {
	props, err := readRecoveryPropertyNames(filepath.Join(s.lsmPath, ".migrations", name))
	if err != nil {
		return nil, false, !os.IsNotExist(err)
	}
	if len(props) == 0 {
		return nil, false, false
	}
	return props, true, false
}
