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

// HasPromotableReindexState reports whether any local shard carries a
// migration generation for (property, indexType) that
// [FinalizeCompletedMigrations] would promote on the next restart. Read-only;
// never loads a shard.
//
// "Promotable" = a tracker dir with tidied.mig or merged.mig, the same set
// cancel cleanup preserves. merged.mig alone triggers promotion and is
// written during PREPARING, so this goes true at the merge, not the swap.
// Unreadable directories answer true, same as [hasStalePartialReindexState].
func (db *DB) HasPromotableReindexState(collection, propName, indexType string) bool {
	idx := db.GetIndex(schema.ClassName(collection))
	if idx == nil {
		return false
	}
	return idx.HasPromotableReindexState(propName, indexType)
}

// HasPromotableReindexState is the per-index half of
// [DB.HasPromotableReindexState]. Stops at the first shard that has such a
// generation.
func (i *Index) HasPromotableReindexState(propName, indexType string) bool {
	var found bool
	// ForEachShard rather than forEachShardStrict: a closing index answers
	// false here, and the caller (repair guidance on a cancelled task) has
	// nothing to act on while the index is going away anyway.
	_ = i.ForEachShard(func(name string, _ ShardLike) error {
		if found {
			return nil
		}
		if hasPromotableReindexState(shardPathLSM(i.path(), name), propName, indexType) {
			found = true
		}
		return nil
	})
	return found
}

// hasPromotableReindexState is the on-disk predicate behind
// [Index.HasPromotableReindexState], for the shard rooted at lsmPath.
//
// Fails closed the same way [hasStalePartialReindexState] does: an
// indexType this build does not recognize, or a .migrations dir it cannot
// enumerate, answers true. An absent dir is the exception and answers
// false — a shard that never ran a migration is the common case.
func hasPromotableReindexState(lsmPath, propName, indexType string) bool {
	prefixes := migrationDirsForPropertyIndex(propName, indexType)
	if len(prefixes) == 0 {
		return true
	}
	if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
		prefixes = append(prefixes, classDir)
	}
	if _, err := os.ReadDir(filepath.Join(lsmPath, ".migrations")); err != nil {
		return !os.IsNotExist(err)
	}
	return len(completedMigrationGens(lsmPath, prefixes)) > 0
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

// classifyCloseCause tags a walk that a close cut short, so the caller can tell
// the two ends apart: a deleted collection leaves nothing behind, anything else
// leaves every shard unswept. Anything that is not a close is passed through
// untouched.
func classifyCloseCause(err error) error {
	switch {
	case errors.Is(err, errIndexDropped):
		return fmt.Errorf("%w: %w", ErrCleanupCollectionDropped, err)
	case errors.Is(err, errIndexShutdown), errors.Is(err, errIndexClosed):
		return fmt.Errorf("%w: %w", ErrCleanupSweepTruncated, err)
	default:
		return err
	}
}

// CleanStalePartialReindexState iterates every local shard of this index
// and calls the per-shard cleanup. Per-shard errors are collected and
// returned together so the caller can decide whether to refuse the submit
// or proceed with a warning.
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
// would remove. Unwrapping every cold shard of a large multi-tenant collection
// hydrates thousands of tenants (LSM store, vector index) under that gate, for
// a sweep that finds nothing on almost all of them.
func (i *Index) CleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
) error {
	var shardErrs error
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
			// Read-only, so it cannot race a concurrent load: the removals
			// still happen through the loaded shard below.
			if !lazy.isLoaded() &&
				!hasStalePartialReindexState(shardPathLSM(i.path(), name), propName, indexType) {
				return nil
			}
			unwrapped, unwrapErr := lazy.Unwrap(ctx)
			if unwrapErr != nil {
				shardErrs = errors.Join(shardErrs,
					fmt.Errorf("shard %q: unwrap for partial-reindex cleanup: %w", name, unwrapErr))
				return nil
			}
			shard = unwrapped
		}
		if err := shard.CleanStalePartialReindexState(ctx, propName, indexType); err != nil {
			shardErrs = errors.Join(shardErrs, fmt.Errorf("shard %q: %w", name, err))
		}
		return nil
	})
	return errors.Join(shardErrs, classifyCloseCause(walkErr))
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
// A directory that is absent is the exception, and answers false. An inactive
// tenant that was never populated has no directory yet, which is the common
// case this whole check exists for. A tenant with no local data because it was
// offloaded is not this case: freezing removes the shard from the index's shard
// map before it removes the files, so the sweep never reaches one.
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
	// sidecar turned up, which on a cold shard it almost never does.
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

	// Migration tracker dirs, minus the deferred-finalize generations.
	prefixes := migrationDirsForPropertyIndex(propName, indexType)
	preservedGens := completedMigrationGens(lsmPath, prefixes)
	entries, err = os.ReadDir(filepath.Join(lsmPath, ".migrations"))
	if err != nil {
		return !os.IsNotExist(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || !isMigrationDirOf(entry.Name(), prefixes) {
			continue
		}
		if _, gen, ok := parseMigrationDirName(entry.Name()); ok && preservedGens[gen] {
			continue
		}
		return true
	}
	return false
}
