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

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
)

// CleanStalePartialReindexState wipes any on-disk runtime-reindex state for
// the given (collection, property, indexType) tuple across every local shard
// of the collection. It is the CANCEL→retry counterpart to the cleanup that
// updatePropertyBuckets does on DELETE→re-enable.
//
// Call sites:
//
//  1. Cancel handler, AFTER asking DTM to cancel and AFTER the local reindex
//     goroutine has exited. Ensures the next submit starts from a clean slate.
//
//  2. Submit handler, BEFORE submitting a new task, as defense in depth. The
//     cancel-then-cleanup path can be skipped if the node crashed between
//     CancelDistributedTask and the cleanup, leaving stale state on disk.
//     Submit-time cleanup catches that case.
//
// Safe to call when no stale state exists — the per-shard helper is
// idempotent: missing directories and unloaded buckets are silently skipped.
//
// Caller MUST ensure no local reindex goroutine is touching this
// (collection, prop, indexType) when this fires; the cancel handler does
// that via [ReindexProvider.WaitForLocalDrain]. Without the wait, the
// cleanup races against the in-flight worker which is still writing to the
// __reindex / __ingest buckets — the shutdown would tear those buckets out
// from under the writer.
//
// A collection this node does not have is reported with
// [ErrCleanupCollectionDropped], which callers skip via
// [IsCleanupCollectionDropped]. There is nothing to sweep and nothing that
// needs it, but reporting a clean sweep would claim state was cleared.
//
// Production goes through [DB.NewStalePartialReindexSweep], which shares one
// directory cache across a run of sweeps. This form is the single-sweep seam
// for consumers and tests.
func (db *DB) CleanStalePartialReindexState(
	ctx context.Context,
	collection, propName, indexType string,
) error {
	return db.cleanStalePartialReindexState(ctx, collection, propName, indexType, nil)
}

// NewStalePartialReindexSweep returns a [DB.CleanStalePartialReindexState]
// that shares the directory listings the cold-shard gate reads across every
// call made to it. Use it for a run of sweeps over one collection — the submit
// and cancel handlers sweep the same shards once per index type, inside the
// request and under the (collection, property) submit lock, so reading each
// shard's directories once rather than once per index type is the difference
// the caller waits for.
//
// The returned function is not safe for concurrent use, and a run of sweeps
// must be short: it answers from what the filesystem looked like when it first
// looked. See [dirNamesCache] for why staleness there costs a hydration and
// never a skipped shard.
func (db *DB) NewStalePartialReindexSweep() func(ctx context.Context, collection, propName, indexType string) error {
	dirs := &dirNamesCache{}
	return func(ctx context.Context, collection, propName, indexType string) error {
		return db.cleanStalePartialReindexState(ctx, collection, propName, indexType, dirs)
	}
}

// cleanStalePartialReindexState is [DB.CleanStalePartialReindexState] with the
// directory listings the cold-shard gate reads shareable across a run of
// sweeps. A nil cache reads the filesystem every time.
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

// ErrCleanupSweepTruncated marks a sweep that did not visit every shard: it
// ran out of time mid-walk, it started on a closing index and visited none at
// all, it was handed an index type this build cannot map to a bucket, or a
// shard left the map before the walk got to it. The shards it did not reach
// were never looked at, so the caller's answer for them is "unknown" and not
// "these shards failed".
//
// On a multi-tenant collection with activity-based deactivation this is the
// routine outcome, not an anomaly: a HOT→COLD transition removes the tenant
// from the shard map mid-walk, and the walk reports the name it never reached.
// The state on that tenant's disk really is unswept, so the warning is honest.
var ErrCleanupSweepTruncated = errors.New("partial-reindex cleanup did not reach every shard")

// ErrCleanupCollectionDropped marks a sweep that found the collection not on
// this node: a delete that landed before the sweep started or while it walked,
// and equally a collection this node never held. Nothing was swept and nothing
// needs to be: the sidecar state lives under the collection's directory and is
// not here without it, so there is no later sweep to retry either.
//
// Which of the two it was is not something the sweep establishes, so the text
// does not claim a delete is in flight.
var ErrCleanupCollectionDropped = errors.New("partial-reindex cleanup skipped: the collection is not on this node")

// ErrCleanupShardFailed marks a sweep that reached a shard and could not sweep
// it. The other two markers say why a sweep stopped; this one says what it
// found on the way. A delete that lands mid-walk after a shard already failed
// carries both, and a caller that asks only about the delete reports the state
// as gone while that shard's is still on disk.
var ErrCleanupShardFailed = errors.New("partial-reindex cleanup could not sweep every shard it reached")

// IsCleanupCollectionDropped reports whether the collection not being on this
// node is the whole of what a sweep error says. Use it rather than
// [errors.Is](err, [ErrCleanupCollectionDropped]), which also matches a sweep
// that left a shard's state behind before the delete landed.
func IsCleanupCollectionDropped(err error) bool {
	return errors.Is(err, ErrCleanupCollectionDropped) && !errors.Is(err, ErrCleanupShardFailed)
}

// classifyCloseCause tags a walk that did not reach every shard, so the caller
// can tell the two ends apart: a collection being deleted leaves nothing
// behind, anything else leaves shards unswept. Anything else is passed through
// untouched.
func classifyCloseCause(err error) error {
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

// CleanStalePartialReindexState iterates every local shard of this index
// and calls the per-shard cleanup. Per-shard errors are collected and
// returned together, capped at [maxReportedErrors], so the caller can decide
// whether to refuse the submit or proceed with a warning.
//
// Errors do NOT stop iteration: a stuck shard must not prevent the other
// shards from being cleaned, otherwise a one-shard failure would
// permanently wedge the (collection, prop, indexType) tuple at every
// future submit. Context cancellation DOES stop it: every caller blocks
// something for the whole sweep — [ReindexProvider.autoCleanupAfterTerminal]
// registers the task's shards in cleanupInProgress, which the backup gate
// consults, and the REST submit and cancel handlers hold the
// (collection, property) submit mutex — so the work left after the deadline
// has to end rather than continue as a run of failed loads. That abort is
// joined into the returned error and tagged with
// [ErrCleanupSweepTruncated], because a caller that sees only the shard
// failures reads a bounded problem where the truth is that the sweep stopped.
//
// A closing index is also truncated, not clean: the shard walk visits nothing
// at all there, so reporting success would tell the caller every shard was
// swept when none was. A collection being deleted is neither, and is tagged
// with [ErrCleanupCollectionDropped]: its state is deleted along with it.
//
// A shard that is not loaded is loaded only if it has on-disk state this sweep
// would remove. Unwrapping every unhydrated shard of a large multi-tenant
// collection hydrates thousands of tenants (LSM store, vector index) under that
// gate, for a sweep that finds nothing on almost all of them.
//
// Production goes through [DB.NewStalePartialReindexSweep]. This form is the
// single-sweep seam for consumers and tests; the taxonomy above is implemented
// by the private cleanStalePartialReindexState both of them call.
func (i *Index) CleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
) error {
	return i.cleanStalePartialReindexState(ctx, propName, indexType, nil)
}

// cleanStalePartialReindexState is [Index.CleanStalePartialReindexState] with
// the directory listings the cold-shard gate reads shareable across a run of
// sweeps. A nil cache reads the filesystem every time.
func (i *Index) cleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
	dirs *dirNamesCache,
) error {
	// An index type this build cannot map to a bucket is refused before the walk
	// rather than per shard: every shard would refuse it identically, and on an
	// all-cold collection the gate would skip them all and leave the sweep
	// reporting a clean run for an input it never processed.
	if _, ok := mainBucketForPropertyIndex(propName, indexType); !ok {
		return fmt.Errorf("%w: unknown indexType %q", ErrCleanupSweepTruncated, indexType)
	}
	// The rendered message is capped at [maxReportedErrors] for the same reason
	// the unvisited-shard list is: the failures that scale with a node's tenant
	// count are the ones a full disk produces on every shard at once. The
	// compounder itself still holds one error per failed shard.
	shardErrs := errorcompounder.New()
	// forEachShardStrict rather than ForEachShard, which answers a closing
	// index with a silent nil that is indistinguishable here from a sweep that
	// reached every shard.
	walkErr := i.forEachShardStrict(func(name string, shardLike ShardLike) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: stopped before shard %q: %w", ErrCleanupSweepTruncated, name, ctxErr)
		}
		shard, ok := shardLike.(*Shard)
		if !ok {
			lazy, isLazy := shardLike.(*LazyLoadShard)
			if !isLazy {
				// An unrecognized wrapper is skipped rather than failed: the
				// post-restart finalize and the OnAfterLsmInitAsync stale
				// sentinel check both fire when that shard is next loaded, so
				// the safety net still catches whatever is left here.
				return nil
			}
			// A shard with nothing on disk has nothing this sweep owns, so
			// skipping it loses nothing: state a concurrently starting task
			// writes after the check belongs to that task and must not be
			// swept. The OnAfterLsmInitAsync stale sentinel check named above
			// is the backstop for whatever slips through.
			if !lazy.isLoaded() &&
				!hasStalePartialReindexState(shardPathLSM(i.path(), name), propName, indexType, dirs) {
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
	return errors.Join(failedShards, classifyCloseCause(walkErr))
}

// hasStalePartialReindexState reports whether the shard rooted at lsmPath has
// any on-disk state [Shard.CleanStalePartialReindexState] would remove for this
// (propName, indexType). Read-only: it never loads the shard.
//
// Answers true on anything it cannot read. A directory it could not enumerate
// is a question it could not ask, and the sweep it gates reports whatever it
// then finds; guessing "nothing to clean" would leave a stale started.mig for
// the next task to resume against.
//
// That includes an index type this build cannot map to a bucket. The sweep this
// gates refuses that input before the walk starts, so the answer here is never
// read in production; answering true keeps the two consistent if that ever
// changes, rather than turning an unprocessable input into a clean sweep.
//
// The shards this saves are the HOT tenants that have not been hydrated yet:
// an inactive tenant is not in the index's shard map at all, so the sweep never
// reaches one. For most of them the directory exists and holds no reindex
// state, which is the common answer. A directory that is absent is the
// exception — a shard nothing has written to yet — and answers false. A tenant
// with no local data because it was offloaded is not that case either: the
// FROZEN transition removes the shard from the shard map before it removes the
// files, so a shard whose files are already gone is one the walk cannot hand
// over. One the walk handed over just before the transition landed reads a
// directory being emptied, and answers whatever it finds — a hydration this
// sweep then fails on, never a silent skip.
func hasStalePartialReindexState(lsmPath, propName, indexType string, dirs *dirNamesCache) bool {
	mainBucketName, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return true
	}

	names, err := dirs.listSidecarCandidates(lsmPath)
	if err != nil {
		return !os.IsNotExist(err)
	}
	var sidecarSuffixes []string
	for _, name := range names {
		if isSidecarDirOf(name, mainBucketName) {
			sidecarSuffixes = append(sidecarSuffixes, strings.TrimPrefix(name, mainBucketName))
		}
	}
	scope := migrationDirsOf(lsmPath, dirs, propName, indexType)
	// Sidecar bucket dirs, minus the ones backing a completed-but-deferred
	// migration — those are live state the sweep preserves. The preserve set is
	// only computed once a candidate sidecar turned up. That is not the rare
	// branch it looks like: a class-level migration awaiting finalize leaves a
	// live sidecar on every tenant of the collection, and that window lasts
	// until the next restart. It reads the shard's .migrations dir through the
	// same cache the rest of this run uses, so it costs one listing per shard.
	if len(sidecarSuffixes) > 0 {
		preserveSidecars := completedMigrationSidecarSuffixes(scope.preserving(indexType))
		for _, suffix := range sidecarSuffixes {
			if !preserveSidecars[suffix] {
				return true
			}
		}
	}

	// Migration tracker dirs, minus the deferred-finalize generations.
	names, err = dirs.list(filepath.Join(lsmPath, ".migrations"))
	if err != nil {
		return !os.IsNotExist(err)
	}
	var preservedGens map[int]bool
	for _, name := range names {
		if !scope.matches(name) {
			continue
		}
		if preservedGens == nil {
			preservedGens = completedMigrationGens(scope)
		}
		if _, gen, ok := parseMigrationDirName(name); ok && preservedGens[gen] {
			continue
		}
		return true
	}
	return false
}

// maxCachedDirNames bounds what one [dirNamesCache] holds. A node runs tens of
// thousands of tenants, and keeping every one of their listings alive for a
// whole cleanup is the memory the cold-shard gate exists not to spend.
//
// Every listing costs one, plus one per name kept. Charging the listing is what
// makes this bound the workload it is for: an untouched tenant contributes no
// names at all, so counting names alone would let a node's whole tenant set in
// for free as map entries. A tenant mid-migration adds a handful on top.
//
// A run that reaches the bound stops admitting rather than evicting: the sweeps
// that follow read the filesystem, which is what they did before this cache.
const maxCachedDirNames = 100_000

// dirNamesCache remembers the directory names one cold-shard gate asked about,
// so a run of sweeps over the same shards reads each of them once instead of
// once per sweep. A nil cache reads the filesystem every time; the zero value
// caches.
//
// A cached listing can be stale — a sweep between two lookups removes dirs the
// second one still sees — and that is safe in one direction only: a name that
// is gone makes the gate answer "there is state here", which costs a hydration
// and never skips a shard that had state. It cannot go the other way, because a
// sweep that removed anything hydrated that shard, and a hydrated shard is
// swept unconditionally — the gate is not asked about it again.
//
// Not safe for concurrent use; the sweeps sharing one run do so in sequence.
type dirNamesCache struct {
	listings map[dirNamesKey]dirNamesListing
	// cost is what the listings are charged against [maxCachedDirNames].
	cost int
}

// dirNamesKey identifies one cached answer. The filter is part of it because
// what is cached is the answer and not the directory: a full listing and a
// sidecar-filtered one of the same path are different answers, and a caller
// that got the filtered one back for an unfiltered question would silently miss
// every bucket dir.
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
// could be a sidecar of some bucket. A sidecar is "<mainBucket>__<suffix>", so
// a name without "__" is not one whatever bucket it is asked about, which makes
// this the one filter that holds for every (property, index type) sharing a
// cache. Dropping the rest is what keeps an untouched shard's entry empty: a
// shard's LSM dir holds one directory per bucket, and a class with 20 indexed
// properties has ~100 of them.
//
// This and [dirNamesCache.list] answer different questions about the same path
// and are kept apart by [dirNamesKey], so either may be asked about any path.
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
	if c.cost+len(names)+1 <= maxCachedDirNames {
		if c.listings == nil {
			c.listings = map[dirNamesKey]dirNamesListing{}
		}
		// Clipped because listDirNames sizes the slice for the whole directory:
		// a filtered listing that kept nothing would otherwise hold a backing
		// array as big as the shard's bucket count for the rest of the run.
		c.listings[key] = dirNamesListing{names: slices.Clip(names), err: err}
		c.cost += len(names) + 1
	}
	return names, err
}

// listDirNames names the directories directly under path that keep accepts; a
// nil keep accepts all of them. The callers only ever ask about directories, so
// the files are dropped here rather than at each of them.
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
