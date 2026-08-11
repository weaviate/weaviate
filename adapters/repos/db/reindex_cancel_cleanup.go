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

// NewStalePartialReindexSweep returns a sweep that wipes any on-disk
// runtime-reindex state for the given (collection, property, indexType) tuple
// across every local shard of the collection. It is the CANCEL→retry
// counterpart to the cleanup that updatePropertyBuckets does on
// DELETE→re-enable.
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
//  3. Background cleanup on every node once a task reaches FAILED or
//     CANCELLED ([autoCleanupAfterTerminal]).
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
// A missing local collection is reported via [ErrCleanupCollectionDropped]
// ([IsCleanupCollectionDropped]) rather than as a clean sweep, since nothing
// was actually cleared.
//
// The returned sweep shares a directory-listing cache across every call, so a
// run of sweeps over one collection reads each shard's directories once
// instead of once per index type. It is therefore not safe for concurrent
// use, and must be short-lived: it answers from a filesystem snapshot taken
// on first read (see [dirNamesCache]).
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

// ErrCleanupSweepTruncated marks a sweep that did not visit every shard: it
// ran out of time, started on a shutting-down or unsignalled-closing index,
// hit an unmappable index type, or a shard left the map before the walk
// reached it. Those shards are "unknown", not "failed" — often benign (e.g. a
// HOT→COLD transition removing a tenant mid-walk), but still unverified.
//
// Callers log it at the severity of what they were about to do: REST
// handlers log Error since a submit or cancel proceeds on possibly-stale
// state; background cleanup logs Warn since the stale-sentinel check on the
// next load is still the backstop.
var ErrCleanupSweepTruncated = errors.New("partial-reindex cleanup did not reach every shard")

// ErrCleanupCollectionDropped marks a sweep that found the collection not on
// this node (deleted, or never held here). Nothing was swept and nothing
// needs to be, since sidecar state lives under the collection's directory.
var ErrCleanupCollectionDropped = errors.New("partial-reindex cleanup skipped: the collection is not on this node")

// ErrCleanupShardFailed marks a sweep that reached a shard and could not
// sweep it. A delete landing mid-walk after a shard already failed carries
// both this and [ErrCleanupCollectionDropped].
var ErrCleanupShardFailed = errors.New("partial-reindex cleanup could not sweep every shard it reached")

// IsCleanupCollectionDropped reports whether the collection being gone is the
// whole of what a sweep error says. Unlike a bare errors.Is check, it returns
// false if a shard also failed before the delete landed.
func IsCleanupCollectionDropped(err error) bool {
	return errors.Is(err, ErrCleanupCollectionDropped) && !errors.Is(err, ErrCleanupShardFailed)
}

// classifyCloseCause tags a walk that did not reach every shard: a collection
// delete as [ErrCleanupCollectionDropped], anything else (shutdown, unvisited
// shards) as [ErrCleanupSweepTruncated]. Other errors pass through untouched.
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

// cleanStalePartialReindexState iterates every local shard of this index
// and calls the per-shard cleanup. Per-shard errors are collected and
// returned together, capped at [maxReportedErrors], so the caller can decide
// whether to refuse the submit or proceed with a warning.
//
// Errors do NOT stop iteration: a stuck shard must not permanently wedge the
// (collection, prop, indexType) tuple. Context cancellation DOES stop it —
// every caller holds a lock or gate for the whole sweep — and is tagged
// [ErrCleanupSweepTruncated] so callers don't mistake a stopped sweep for a
// bounded set of shard failures.
//
// A closing index is truncated too (the walk visits nothing); a deleted
// collection is [ErrCleanupCollectionDropped] instead, since its state is
// gone along with it.
//
// An unloaded shard is only hydrated if it actually has on-disk state to
// remove, to avoid hydrating every unloaded tenant of a large collection.
//
// dirs carries the directory listings the unloaded-shard gate reads across a
// run of sweeps; a nil cache reads the filesystem every time.
func (i *Index) cleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
	dirs *dirNamesCache,
) error {
	// Refused before the walk rather than per shard: the unloaded-shard gate
	// below fails open on an unmappable index type, so per-shard refusal would
	// hydrate every unloaded tenant to reject the same input repeatedly.
	if _, ok := mainBucketForPropertyIndex(propName, indexType); !ok {
		return fmt.Errorf("%w: unknown indexType %q", ErrCleanupSweepTruncated, indexType)
	}
	shardErrs := errorcompounder.New()
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
				// Skipped, not failed: the post-restart finalize and
				// OnAfterLsmInitAsync stale-sentinel check catch it on next load.
				return nil
			}
			// Unloaded and nothing on disk to sweep: skip rather than hydrate,
			// leaving the sentinel check above as the backstop.
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
// on-disk state [Shard.CleanStalePartialReindexState] would remove, without
// loading the shard.
//
// Fails open (returns true) on anything it can't read — an unmappable index
// type, an unlistable directory — since a false "clean" would leave a stale
// started.mig for the next task to resume against.
//
// A FROZEN (offload) transition removes the shard from the map before it
// removes files, so an already-offloaded shard is never handed to this walk;
// one caught mid-transition reads a directory being emptied and skips it,
// which the offload is about to make true anyway.
//
// A deactivated (COLD) tenant is absent from the map too; its on-disk state
// is untouched until reactivation (OnAfterLsmInitAsync's stale-sentinel
// check).
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
	// migration — those are live state the sweep must preserve.
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
// A name removed since caching makes the gate over-report "state here": an
// extra hydration, never a skipped shard.
//
// A name added since caching makes it under-report, so a shard with new
// state may be skipped — bounded by the cache's lifetime (one HTTP request
// or one [reindexTerminalCleanupTimeout] window) and by the stale-sentinel
// check on the shard's next load.
type dirNamesCache struct {
	listings map[dirNamesKey]dirNamesListing
	// cost is what the listings are charged against [maxCachedDirNames].
	cost int
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
	if c.cost+len(names)+1 <= maxCachedDirNames {
		if c.listings == nil {
			c.listings = map[dirNamesKey]dirNamesListing{}
		}
		// Cloned, not clipped: clipping only shrinks the header, so the
		// full-size backing array from listDirNames would stay alive for the
		// rest of the run — even for an unloaded tenant's empty filtered listing.
		c.listings[key] = dirNamesListing{names: slices.Clone(names), err: err}
		c.cost += len(names) + 1
	}
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
