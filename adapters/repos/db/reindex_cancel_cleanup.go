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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
)

// NewStalePartialReindexSweep returns a sweep that wipes on-disk
// runtime-reindex state for one (collection, property, indexType) tuple
// across every local shard — the CANCEL→retry counterpart to the DELETE
// path in updatePropertyBuckets.
//
// Call sites: the cancel handler (after DTM cancel and the local reindex
// goroutine exits), submit-time pre-cleanup (covers a crash between those
// two steps), and background cleanup once a task reaches FAILED/CANCELLED
// ([autoCleanupAfterTerminal]).
//
// Caller MUST ensure no local reindex goroutine is touching this tuple —
// otherwise the cleanup races the worker's writes to the __reindex/__ingest
// buckets. The cancel handler enforces this via
// [ReindexProvider.WaitForLocalTaskDrain].
//
// A missing local collection reports [ErrCleanupCollectionDropped], not a
// clean sweep. The returned sweep caches directory listings across calls, so
// it is not safe for concurrent use and must be short-lived (see
// [dirNamesCache]).
func (db *DB) NewStalePartialReindexSweep() func(ctx context.Context, collection, propName, indexType string) error {
	dirs := &dirNamesCache{}
	return func(ctx context.Context, collection, propName, indexType string) error {
		return db.cleanStalePartialReindexState(ctx, collection, propName, indexType, dirs)
	}
}

// cleanStalePartialReindexState is one sweep of
// [DB.NewStalePartialReindexSweep]. A nil cache reads the filesystem every
// time.
func (db *DB) cleanStalePartialReindexState(
	ctx context.Context,
	collection, propName, indexType string,
	dirs *dirNamesCache,
) error {
	idx := db.GetIndex(schema.ClassName(collection))
	if idx == nil {
		// Reported as dropped rather than clean; see the doc comment above.
		return fmt.Errorf("%w: no local index for collection %q",
			ErrCleanupCollectionDropped, collection)
	}
	return idx.cleanStalePartialReindexState(ctx, propName, indexType, dirs)
}

// ErrCleanupSweepTruncated marks a sweep that didn't visit every shard —
// timeout, a shutting-down/closing index, an unmappable index type, or a
// shard that left the map mid-walk. These shards are "unknown", not
// "failed" (often benign, e.g. a HOT→COLD tenant transition), but still
// unverified.
//
// Callers log it at Error when a submit or cancel proceeds on possibly-stale
// state, and at Warn from background cleanup, which sweeps the tuple again
// before its next submit.
var ErrCleanupSweepTruncated = errors.New("partial-reindex cleanup did not reach every shard")

// ErrCleanupCollectionDropped marks a sweep that found the collection not on
// this node (deleted, or never held here), possibly after sweeping some
// shards before a mid-walk delete. Whatever state remains — including files a
// backup in flight keeps past the drop ([Index.drop]'s keepFiles) — is
// harmless: with the class gone from the schema, no later submit can
// short-circuit on it.
var ErrCleanupCollectionDropped = errors.New("partial-reindex cleanup skipped: the collection is not on this node")

// ErrCleanupShardFailed marks a sweep that reached a shard and could not
// sweep it. A delete landing mid-walk after a shard already failed carries
// both this and [ErrCleanupCollectionDropped].
//
// Exported so callers across the package boundary can classify it with
// errors.Is.
var ErrCleanupShardFailed = errors.New("partial-reindex cleanup could not sweep every shard it reached")

// IsCleanupCollectionDropped reports whether the collection being gone is the
// whole of what a sweep error says. Unlike a bare errors.Is check, it returns
// false if a shard also failed before the delete landed.
func IsCleanupCollectionDropped(err error) bool {
	return errors.Is(err, ErrCleanupCollectionDropped) && !errors.Is(err, ErrCleanupShardFailed)
}

// classifyIncompleteWalk tags a walk that did not reach every shard: a
// collection delete as [ErrCleanupCollectionDropped], anything else (shutdown,
// unvisited shards) as [ErrCleanupSweepTruncated]. Other errors pass through
// untouched.
func classifyIncompleteWalk(err error) error {
	switch {
	case errors.Is(err, errIndexDropped):
		return fmt.Errorf("%w: %w", ErrCleanupCollectionDropped, err)
	case errors.Is(err, errIndexShutdown), errors.Is(err, errIndexClosed),
		errors.Is(err, errShardsSkipped):
		return fmt.Errorf("%w: %w", ErrCleanupSweepTruncated, err)
	default:
		return err
	}
}

// cleanStalePartialReindexState iterates every local shard, collecting
// per-shard errors (capped at [maxReportedErrors]) so the caller can decide
// whether to refuse the submit or proceed with a warning.
//
// A stuck shard does NOT stop iteration — that would wedge the tuple at
// every future submit — but context cancellation does, tagged
// [ErrCleanupSweepTruncated] so it isn't mistaken for a bounded set of
// shard failures. A closing index is truncated the same way; a deleted
// collection is [ErrCleanupCollectionDropped] instead, since what it leaves
// behind is harmless.
//
// An unloaded shard is only hydrated if it actually has on-disk state to
// remove. dirs caches directory listings across the run; nil reads the
// filesystem every time.
//
// A walk that starts leaves exactly one summary line naming its outcome, at
// the level that outcome warrants (see [sweepSummary]).
func (i *Index) cleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
	dirs *dirNamesCache,
) error {
	// Refused before the walk, not per shard: per-shard refusal would
	// hydrate every unloaded tenant just to reject the same input.
	if _, ok := mainBucketForPropertyIndex(propName, indexType); !ok {
		return fmt.Errorf("%w: unknown indexType %q", ErrCleanupSweepTruncated, indexType)
	}
	shardErrs := errorcompounder.New()
	skippedShards, payloadReads := 0, 0
	// forEachShardStrict, not ForEachShard: a closing index must not read as a
	// sweep that reached every shard.
	walkErr := i.forEachShardStrict(func(name string, shardLike ShardLike) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: stopped before shard %q: %w", ErrCleanupSweepTruncated, name, ctxErr)
		}
		shard, ok := shardLike.(*Shard)
		if !ok {
			lazy, isLazy := shardLike.(*LazyLoadShard)
			if !isLazy {
				// Unreachable in production (only two implementations exist).
				shardErrs.Add(fmt.Errorf(
					"shard %q: partial-reindex cleanup cannot sweep a %T", name, shardLike))
				return nil
			}
			// Charged whichever way the gate answers: the reads are paid before
			// it decides, so billing only the hydrating half reports zero exactly
			// where a node full of cold tenants pays the most.
			skip, gateReads := lazy.canSkipUnloadedSweep(propName, indexType, dirs)
			payloadReads += gateReads
			// Unloaded and nothing on disk to sweep: skip rather than hydrate.
			if skip {
				skippedShards++
				return nil
			}
			unwrapped, unwrapErr := lazy.Unwrap(ctx)
			if unwrapErr != nil {
				shardErrs.Add(
					fmt.Errorf("shard %q: unwrap for partial-reindex cleanup: %w", name, unwrapErr))
				return nil
			}
			shard = unwrapped
		}
		if err := shard.CleanStalePartialReindexState(ctx, propName, indexType); err != nil {
			shardErrs.Add(fmt.Errorf("shard %q: %w", name, err))
		}
		return nil
	})
	var failedShards error
	if reported := shardErrs.ToErrorLimited(maxReportedErrors); reported != nil {
		failedShards = fmt.Errorf("%w: %w", ErrCleanupShardFailed, reported)
	}
	sweepErr := errors.Join(failedShards, classifyIncompleteWalk(walkErr))

	outcome, _ := ClassifyCleanupSweep(sweepErr)
	msg, level := sweepSummary(outcome)
	uncachedListings := dirs.refusedListings()
	if uncachedListings > 0 {
		// logrus orders its levels descending, so this only ever raises severity:
		// a bound the cache silently hit has no other signal.
		level = min(level, logrus.WarnLevel)
	}
	i.logger.WithFields(map[string]any{
		"property":          propName,
		"index_type":        indexType,
		"operation":         "CleanStalePartialReindexState",
		"skipped_shards":    skippedShards,
		"payload_reads":     payloadReads,
		"uncached_listings": uncachedListings,
	}).Log(level, msg)
	return sweepErr
}

// sweepSummary is the one line [Index.cleanStalePartialReindexState] leaves per
// sweep: what the sweep left behind, at the level that outcome warrants. Only a
// shard the sweep reached and could not sweep is known to still hold state,
// which is why it alone is an error.
func sweepSummary(outcome CleanupSweepOutcome) (msg string, level logrus.Level) {
	switch outcome {
	case CleanupSweepFailed:
		return "partial-reindex cleanup: a shard could not be swept, so the partial state on it is still there",
			logrus.ErrorLevel
	case CleanupSweepUnknown:
		return "partial-reindex cleanup: the sweep did not reach every shard, so any partial state on the ones it missed is still there",
			logrus.WarnLevel
	case CleanupSweepDropped:
		return "partial-reindex cleanup: the collection is not on this node, so whatever is left here goes with the collection directory",
			logrus.InfoLevel
	default:
		return "partial-reindex cleanup: sweep finished, unloaded shards with nothing to sweep left unloaded",
			logrus.InfoLevel
	}
}

// hasStalePartialReindexState reports whether the shard rooted at lsmPath
// has on-disk state [Shard.CleanStalePartialReindexState] would remove,
// without loading the shard.
//
// Fails open (returns true) on anything it can't read — an unmappable index
// type, an unlistable directory, or an unparseable tracker payload — since a
// false "clean" would leave a stale started.mig for the next task to resume
// against.
//
// The unreadable payload only fails open while no properties.mig rebuilds
// the dir's name. Where one does, [readTaskProps] answers from it, and a
// tracker naming other properties leaves this reporting clean and skipping
// the shard.
//
// Failing open costs only a hydration, except on an unlistable .migrations:
// that hydration then finds no completed migration to preserve and removes
// sidecars a deferred finalize still needs.
//
// A FROZEN (offload) transition removes the shard from the map before it
// removes files, so a mid-transition read either finds an emptying
// directory and skips it (which offload is about to make true anyway), or
// races the other way into a spurious [ErrCleanupShardFailed] — never
// corruption. A deactivated (COLD) tenant is absent from the map too, and
// reactivating it changes nothing: the stale-sentinel check runs from the
// task path, not from a shard load.
//
// The second return is the tracker-payload read count for the caller's log
// line; payloads are memoized per call, not shared across shards, since no
// two shards name the same path.
func hasStalePartialReindexState(
	lsmPath, propName, indexType string, dirs *dirNamesCache,
) (bool, int) {
	props := &taskPropsCache{}
	mainBucketName, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return true, props.count()
	}

	names, err := dirs.listSidecarCandidates(lsmPath)
	if err != nil {
		return !os.IsNotExist(err), props.count()
	}
	var sidecarSuffixes []string
	for _, name := range names {
		if isSidecarDirOf(name, mainBucketName) {
			sidecarSuffixes = append(sidecarSuffixes, strings.TrimPrefix(name, mainBucketName))
		}
	}
	scope := migrationDirsOf(lsmPath, dirs, propName, indexType).cachingProps(props)
	// Sidecar bucket dirs, minus the ones backing a completed-but-deferred
	// migration — those are live state the sweep must preserve.
	if len(sidecarSuffixes) > 0 {
		preserveSidecars := completedMigrationSidecarSuffixes(scope.preserving(indexType))
		for _, suffix := range sidecarSuffixes {
			if !preserveSidecars[suffix] {
				return true, props.count()
			}
		}
	}

	// Migration tracker dirs, minus the deferred-finalize generations.
	names, err = dirs.list(filepath.Join(lsmPath, ".migrations"))
	if err != nil {
		return !os.IsNotExist(err), props.count()
	}
	var preservedGens map[int]bool
	for _, name := range names {
		matched, unreadablePayload := scope.match(name)
		if unreadablePayload {
			// A payload this gate can't read could name this property; only
			// hydrating and re-reading can tell, so this is not "clean".
			return true, props.count()
		}
		if !matched {
			continue
		}
		if preservedGens == nil {
			preservedGens = completedMigrationGens(scope)
		}
		if _, gen, ok := parseMigrationDirName(name); ok && preservedGens[gen] {
			continue
		}
		return true, props.count()
	}
	return false, props.count()
}

// maxCachedDirNames bounds what one [dirNamesCache] holds, so a node with tens
// of thousands of tenants doesn't keep every listing alive for a whole
// cleanup. Each listing costs 1 plus 1 per name kept; a listing that would
// overflow the bound is not admitted, and the sweeps that ask for it read the
// filesystem instead.
const maxCachedDirNames = 100_000

// dirNamesCache remembers directory listings across a run of sweeps over the
// same shards, so each is read once instead of once per sweep. A nil cache
// reads the filesystem every time; the zero value caches. Not safe for
// concurrent use.
//
// Staleness cuts both ways: a name removed since caching costs an extra
// hydration, and a name added since caching makes the gate skip a shard that
// has since acquired the very state the sweep exists to remove. Both are
// bounded by the cache's lifetime (one HTTP request or one
// [reindexTerminalCleanupTimeout] window) and caught by the next submit's
// fresh sweep.
type dirNamesCache struct {
	listings map[dirNamesKey]dirNamesListing
	// cost is what the listings are charged against [maxCachedDirNames].
	cost int
	// refused counts the listings the bound kept out, which the sweep reports
	// so a cache that stopped caching is visible.
	refused int
}

// refusedListings reports how many listings [maxCachedDirNames] kept out. A nil
// cache refuses nothing because it admits nothing.
func (c *dirNamesCache) refusedListings() int {
	if c == nil {
		return 0
	}
	return c.refused
}

// dirNamesKey identifies one cached answer. filter is part of the key since a
// full listing and a sidecar-filtered one of the same path are different
// answers.
type dirNamesKey struct {
	path   string
	filter string
}

type dirNamesListing struct {
	names []string
	err   error
}

// list names every directory directly under path.
func (c *dirNamesCache) list(path string) ([]string, error) {
	return c.listMatching(dirNamesKey{path: path}, nil)
}

// listSidecarCandidates names the directories under a shard's LSM path that
// could be a sidecar of some bucket ("<mainBucket>__<suffix>"). This filter
// holds for every (property, index type) asked about the same path, which is
// why it's cached separately from the unfiltered [dirNamesCache.list].
func (c *dirNamesCache) listSidecarCandidates(lsmPath string) ([]string, error) {
	return c.listMatching(dirNamesKey{path: lsmPath, filter: "sidecar"}, func(name string) bool {
		return strings.Contains(name, "__")
	})
}

func (c *dirNamesCache) listMatching(key dirNamesKey, keep func(string) bool) ([]string, error) {
	if c == nil {
		return listDirNames(key.path, keep)
	}
	if listing, ok := c.listings[key]; ok {
		return listing.names, listing.err
	}
	names, err := listDirNames(key.path, keep)
	if c.cost+len(names)+1 > maxCachedDirNames {
		c.refused++
		return names, err
	}
	if c.listings == nil {
		c.listings = map[dirNamesKey]dirNamesListing{}
	}
	// Cloned, not clipped: clipping only shrinks the header, so the full
	// backing array from listDirNames would otherwise stay alive for the
	// rest of the run, even for an empty filtered listing.
	c.listings[key] = dirNamesListing{names: slices.Clone(names), err: err}
	c.cost += len(names) + 1
	return names, err
}

// listDirNames names the directories directly under path that keep accepts
// (nil accepts all).
func listDirNames(path string, keep func(string) bool) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if keep != nil && !keep(entry.Name()) {
			continue
		}
		names = append(names, entry.Name())
	}
	return names, nil
}
