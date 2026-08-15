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

// StalePartialReindexSweep wipes on-disk runtime-reindex state for one
// (collection, property, indexType) tuple across every local shard.
//
// One value belongs to one goroutine and must not outlive the request or
// timeout window that built it: it caches directory listings across calls
// (see [dirNamesCache]).
//
// Caller MUST ensure no local reindex goroutine is touching the tuple —
// otherwise the cleanup races the worker's writes to the __reindex/__ingest
// buckets. The cancel handler enforces this via
// [ReindexProvider.WaitForLocalTaskDrain].
type StalePartialReindexSweep func(ctx context.Context, collection, propName, indexType string) error

// NewStalePartialReindexSweep returns the CANCEL→retry counterpart to the
// DELETE path in updatePropertyBuckets.
//
// Call sites: the cancel handler (after DTM cancel and the local reindex
// goroutine exits), submit-time pre-cleanup (covers a crash between those
// two steps), and background cleanup once a task reaches FAILED/CANCELLED
// ([autoCleanupAfterTerminal]).
//
// A missing local collection reports [ErrCleanupCollectionDropped], not a
// clean sweep.
//
// It raises the cleanup hold here, not at each caller: what it deletes is what a
// backup copies, and two of the three callers leave the other gate open.
func (db *DB) NewStalePartialReindexSweep() StalePartialReindexSweep {
	dirs := &dirNamesCache{}
	return func(ctx context.Context, collection, propName, indexType string) error {
		var err error
		db.reindexHolds.Hold(collection, ReindexHoldCleanup, func() {
			err = db.cleanStalePartialReindexState(ctx, collection, propName, indexType, dirs)
		})
		return err
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

// ErrCleanupSweepTruncated marks a sweep that didn't get through every shard —
// timeout, a shutting-down/closing index, an unmappable index type, or a
// shard that left the map mid-walk. These shards are "unknown", not
// "failed" (often benign, e.g. a HOT→COLD tenant transition), but still
// unverified: a shard the run stopped partway through may be partly swept.
//
// Every caller logs it at Warn: unvisited shards are unverified rather than
// known bad, and a healthy node produces them from routine tenant churn.
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

// truncatedByCancellation re-reports a shard error that a cancelled context
// caused as [ErrCleanupSweepTruncated], and returns nil for every other error.
// A run the context stopped confirms nothing about the shard it stopped on, so
// reporting that as a shard that failed would page an operator over a timeout.
// It stops the walk for the same reason the check at the top of the next
// shard's turn does — the context is gone either way. The two checks read
// different keys on purpose: that one asks whether the next shard may start,
// so it reads the clock; this one asks what stopped a shard, so it reads the
// error's cause — the clock would re-badge a shard that broke for a reason of
// its own as a timeout.
//
// The match is a sentinel test, not provenance: any chain reaching
// context.Canceled or context.DeadlineExceeded matches, an inner timeout's or
// a wrapped cause's (entities/errors.CanceledCause) included. That is sound
// here because the two steps this guards carry no context but the sweep's:
// the shard load's permit wait reports this context's cancellation, and the
// sidecar shutdown hands the context to the bucket's shutdown, whose
// compaction and flush waits both wrap it. Both are pinned, since a step that
// started swallowing the cause would silently turn every cancelled run back
// into a broken shard. A cancellation surfacing deeper, inside NewShard, is
// flattened to a string in [LazyLoadShard.Load] and reads as a shard failure —
// an Error-level false alarm on that arm, accepted over masking a real failure
// as a timeout.
func truncatedByCancellation(reported error) error {
	if !errors.Is(reported, context.Canceled) && !errors.Is(reported, context.DeadlineExceeded) {
		return nil
	}
	return fmt.Errorf("%w: the run's context ended before the sweep finished: %w", ErrCleanupSweepTruncated, reported)
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
// An unloaded shard is only hydrated if its disk asks for it — state to
// remove, or a completed migration's leftovers only a load reclaims (see
// [LazyLoadShard.canSkipUnloadedSweep]). A walk that starts leaves exactly one
// summary line naming its outcome, at the level that outcome warrants (see
// [CleanupSweepSummary]).
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
	// One cache serves every sweep of a request, so only the delta belongs to
	// this one; its running total would re-report the first sweep's refusals.
	refusedBefore := dirs.refusedListings()
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
			skip, gateReads := lazy.canSkipUnloadedSweep(propName, indexType, dirs, dirs.trackerProps())
			payloadReads += gateReads
			// Unloaded and nothing on disk to sweep or reclaim: skip rather
			// than hydrate.
			if skip {
				skippedShards++
				return nil
			}
			unwrapped, unwrapErr := lazy.Unwrap(ctx)
			if unwrapErr != nil {
				reported := fmt.Errorf(
					"shard %q: unwrap for partial-reindex cleanup: %w", name, unwrapErr)
				if truncated := truncatedByCancellation(reported); truncated != nil {
					return truncated
				}
				shardErrs.Add(reported)
				return nil
			}
			shard = unwrapped
		}
		// Charged whether or not the sweep then failed, for the same reason the
		// gate's reads are: the reads are paid before the outcome is known.
		shardReads, err := shard.CleanStalePartialReindexState(ctx, propName, indexType)
		payloadReads += shardReads
		if err != nil {
			reported := fmt.Errorf("shard %q: %w", name, err)
			if truncated := truncatedByCancellation(reported); truncated != nil {
				return truncated
			}
			shardErrs.Add(reported)
		}
		return nil
	})
	var failedShards error
	if reported := shardErrs.ToErrorLimited(maxReportedErrors); reported != nil {
		failedShards = fmt.Errorf("%w: %w", ErrCleanupShardFailed, reported)
	}
	sweepErr := errors.Join(failedShards, classifyIncompleteWalk(walkErr))

	outcome, _ := ClassifyCleanupSweep(sweepErr)
	msg, level := CleanupSweepSummary(sweepPhaseIndexCleanup, outcome)
	uncachedListings := dirs.refusedListings() - refusedBefore
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
// The second return says the shard holds a completed migration's leftovers:
// its data still under the ingest sidecar name, plus the backup copy of the
// bucket it replaced. Only a shard load reclaims those, since
// [FinalizeCompletedMigrations] runs before buckets open. It is only
// meaningful where the first return is false — a shard already being
// hydrated finalizes them on the way in either way.
//
// props memoizes the tracker payloads read on the way to that answer. Callers
// running a grid of tuples over the same shards hand in one for the whole run
// ([dirNamesCache.trackerProps]); a nil one is memoized for this call alone.
func hasStalePartialReindexState(
	lsmPath, propName, indexType string, dirs *dirNamesCache, props *taskPropsCache,
) (stale, finalizable bool) {
	if props == nil {
		// No run-wide memo: keep the passes below sharing one of their own.
		props = &taskPropsCache{}
	}
	mainBucketName, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return true, false
	}

	names, err := dirs.listSidecarCandidates(lsmPath)
	if err != nil {
		return !os.IsNotExist(err), false
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
				return true, false
			}
		}
		// Every sidecar here backs a completed migration, so nothing but a
		// load reclaims them.
		finalizable = true
	}

	// Migration tracker dirs, minus the deferred-finalize generations.
	names, err = dirs.list(filepath.Join(lsmPath, ".migrations"))
	if err != nil {
		return !os.IsNotExist(err), false
	}
	var preservedGens map[int]bool
	for _, name := range names {
		matched, unreadablePayload := scope.inScopeFailingOpen(name)
		if unreadablePayload {
			// A payload this gate can't read could name this property; only
			// hydrating and re-reading can tell, so this is not "clean".
			return true, false
		}
		if !matched {
			continue
		}
		if preservedGens == nil {
			preservedGens = completedMigrationGens(scope)
		}
		if _, gen, ok := parseMigrationDirName(name); ok && preservedGens[gen] {
			finalizable = true
			continue
		}
		return true, false
	}
	return false, finalizable
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
	// props is the tracker-payload memo of the same run; see
	// [dirNamesCache.trackerProps].
	props taskPropsCache
}

// trackerProps is the payload memo sharing this cache's lifetime, so the two
// can never drift apart: every tuple of one run asks the same unloaded shards,
// and a payload costs orders of magnitude more to parse than a listing costs
// to read.
//
// Safe across tuples in both directions: a skipped shard is unchanged, and a
// hydrated one answers from [LazyLoadShard.loaded] before it consults the
// memo again. A nil cache has no memo; its callers keep one per call instead.
func (c *dirNamesCache) trackerProps() *taskPropsCache {
	if c == nil {
		return nil
	}
	return &c.props
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
