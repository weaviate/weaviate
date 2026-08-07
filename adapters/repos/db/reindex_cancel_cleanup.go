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

// ErrCleanupSweepTruncated marks a sweep that stopped before it had visited
// every shard. The shards after that point were never looked at, so the
// caller's answer is "unknown from here on" and not "these shards failed".
var ErrCleanupSweepTruncated = errors.New("partial-reindex cleanup did not reach every shard")

// CleanStalePartialReindexState iterates every local shard of this index
// and calls the per-shard cleanup. Per-shard errors are collected and
// returned together so the caller can decide whether to refuse the submit
// or proceed with a warning.
//
// Errors do NOT stop iteration: a stuck shard must not prevent the other
// shards from being cleaned, otherwise a one-shard failure would
// permanently wedge the (collection, prop, indexType) tuple at every
// future submit. Context cancellation DOES stop it — both call sites hold
// the collection's backup and restore gate closed for the whole sweep — and
// is tagged with [ErrCleanupSweepTruncated] so the caller can tell "sweep
// stopped early" from "these shards failed".
//
// A cold shard is only unwrapped if it has on-disk state this sweep would
// remove, so a large multi-tenant collection doesn't hydrate thousands of
// idle tenants under the gate.
func (i *Index) CleanStalePartialReindexState(
	ctx context.Context,
	propName, indexType string,
) error {
	var shardErrs error
	walkErr := i.ForEachShard(func(name string, shardLike ShardLike) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: stopped before shard %q: %w", ErrCleanupSweepTruncated, name, ctxErr)
		}
		shard, ok := shardLike.(*Shard)
		if !ok {
			lazy, isLazy := shardLike.(*LazyLoadShard)
			if !isLazy {
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
	return errors.Join(shardErrs, walkErr)
}

// hasStalePartialReindexState reports whether the shard rooted at lsmPath has
// any on-disk state [Shard.CleanStalePartialReindexState] would remove for this
// (propName, indexType). Read-only: it never loads the shard.
//
// Answers true on anything it cannot read. A directory it could not enumerate
// is a question it could not ask, and the sweep it gates reports whatever it
// then finds; guessing "nothing to clean" would leave a stale started.mig for
// the next task to resume against.
func hasStalePartialReindexState(lsmPath, propName, indexType string) bool {
	mainBucketName, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return true
	}

	// Sidecar bucket dirs, minus the ones backing a completed-but-deferred
	// migration — those are live state the sweep preserves.
	preservePrefixes := migrationDirsForPropertyIndex(propName, indexType)
	if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
		preservePrefixes = append(preservePrefixes, classDir)
	}
	preserveSidecars := completedMigrationSidecarSuffixes(lsmPath, preservePrefixes)
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return !os.IsNotExist(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || !isSidecarDirOf(entry.Name(), mainBucketName) {
			continue
		}
		if preserveSidecars[strings.TrimPrefix(entry.Name(), mainBucketName)] {
			continue
		}
		return true
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
