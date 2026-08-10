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
func (db *DB) CleanStalePartialReindexState(
	ctx context.Context,
	collection, propName, indexType string,
) error {
	idx := db.GetIndex(schema.ClassName(collection))
	if idx == nil {
		// Collection doesn't exist locally. Nothing to clean.
		return nil
	}
	return idx.CleanStalePartialReindexState(ctx, propName, indexType)
}

// ErrCleanupSweepTruncated marks a sweep that stopped before it had visited
// every shard. The shards after that point were never looked at, so the
// caller's answer is "unknown from here on" and not "these shards failed".
var ErrCleanupSweepTruncated = errors.New("partial-reindex cleanup did not reach every shard")

// ErrCleanupCollectionDropped marks a sweep that ran while the collection was
// being deleted. Nothing was swept and nothing needs to be: the sidecar state
// lives under the collection's directory and goes away with it, so there is no
// later sweep to retry either.
var ErrCleanupCollectionDropped = errors.New("partial-reindex cleanup skipped: the collection is being deleted")

// ErrCleanupShardFailed marks a sweep that reached a shard and could not sweep
// it. The other two markers say why a sweep stopped; this one says what it
// found on the way. A delete that lands mid-walk after a shard already failed
// carries both, and a caller that asks only about the delete reports the state
// as gone while that shard's is still on disk.
var ErrCleanupShardFailed = errors.New("partial-reindex cleanup could not sweep every shard it reached")

// IsCleanupCollectionDropped reports whether the collection being deleted is
// the whole of what a sweep error says. Use it rather than
// [errors.Is](err, [ErrCleanupCollectionDropped]), which also matches a sweep
// that left a shard's state behind before the delete landed.
func IsCleanupCollectionDropped(err error) bool {
	return errors.Is(err, ErrCleanupCollectionDropped) && !errors.Is(err, ErrCleanupShardFailed)
}

// classifyCloseCause tags a walk that did not reach every shard, so the caller
// can tell the two ends apart: a deleted collection leaves nothing behind,
// anything else leaves shards unswept. Anything else is passed through
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
func (i *Index) CleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
) error {
	// Capped at [maxReportedErrors] for the same reason the unvisited-shard
	// list is: the failures that scale with a node's tenant count are the ones
	// a full disk produces on every shard at once.
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
				!hasStalePartialReindexState(shardPathLSM(i.path(), name), propName, indexType) {
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
func hasStalePartialReindexState(lsmPath, propName, indexType string) bool {
	mainBucketName, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return true
	}

	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return !os.IsNotExist(err)
	}
	var sidecarSuffixes []string
	for _, entry := range entries {
		if entry.IsDir() && isSidecarDirOf(entry.Name(), mainBucketName) {
			sidecarSuffixes = append(sidecarSuffixes, strings.TrimPrefix(entry.Name(), mainBucketName))
		}
	}
	// Sidecar bucket dirs, minus the ones backing a completed-but-deferred
	// migration — those are live state the sweep preserves. The preserve set
	// costs its own .migrations walk, so it is only computed once a candidate
	// sidecar turned up, which on an untouched shard it almost never does.
	if len(sidecarSuffixes) > 0 {
		preservePrefixes := migrationDirsForPropertyIndex(propName, indexType)
		if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
			preservePrefixes = append(preservePrefixes, classDir)
		}
		preserveSidecars := completedMigrationSidecarSuffixes(lsmPath, preservePrefixes)
		for _, suffix := range sidecarSuffixes {
			if !preserveSidecars[suffix] {
				return true
			}
		}
	}

	// Migration tracker dirs, minus the deferred-finalize generations. The
	// preserve set is deferred the same way the sidecar one is: it walks
	// .migrations a second time, and a shard with no tracker dir for this
	// property never needs the answer.
	prefixes := migrationDirsForPropertyIndex(propName, indexType)
	entries, err = os.ReadDir(filepath.Join(lsmPath, ".migrations"))
	if err != nil {
		return !os.IsNotExist(err)
	}
	var preservedGens map[int]bool
	for _, entry := range entries {
		if !entry.IsDir() || !isMigrationDirOf(entry.Name(), prefixes) {
			continue
		}
		if preservedGens == nil {
			preservedGens = completedMigrationGens(lsmPath, prefixes)
		}
		if _, gen, ok := parseMigrationDirName(entry.Name()); ok && preservedGens[gen] {
			continue
		}
		return true
	}
	return false
}
