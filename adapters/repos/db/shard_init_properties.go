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
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func (s *Shard) initProperties(eg *enterrors.ErrorGroupWrapper, class *models.Class) {
	ctx := context.TODO()

	s.propertyIndices = propertyspecific.Indices{}
	s.geoQueues = make(map[string]*VectorIndexQueue)
	if class == nil {
		return
	}

	s.initPropertyBuckets(ctx, eg, s.lazySegmentLoadingEnabled, class.Properties...)

	eg.Go(func() error {
		return s.addIDProperty(ctx)
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			return s.addTimestampProperties(ctx)
		})
	}

	if s.index.Config.TrackVectorDimensions {
		eg.Go(func() error {
			return s.addDimensionsProperty(ctx)
		})
	}
}

func (s *Shard) initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper,
	lazyLoadSegments bool, props ...*models.Property,
) {
	makeBucketOptions := s.makeDefaultBucketOptions
	if lazyLoadSegments != s.lazySegmentLoadingEnabled {
		makeBucketOptions = s.overwrittenMakeDefaultBucketOptions(lsmkv.WithLazySegmentLoading(lazyLoadSegments))
	}

	for _, prop := range props {
		propCopy := *prop // prevent loop variable capture

		if _, ok := schema.AsNested(prop.DataType); ok {
			// Preview gate — strict skip when off (no goroutine spawned,
			// no buckets created). Pairs with the analyzer skip in
			// adapters/repos/db/inverted/objects.go; even if one leaks past
			// the other, absence of buckets keeps writes inert.
			if !entcfg.NestedFilteringEnabled() {
				continue
			}
			// TODO aliszka:nested_filtering respect top-level HasAnyInvertedIndex for
			// nested properties before creating buckets — currently bypassed because
			// the interaction between top-level and per-nested-property index settings
			// needs design discussion first (same issue tracked in objects.go).
			eg.Go(func() error {
				return s.createNestedPropertyBuckets(ctx, &propCopy, makeBucketOptions)
			})
			continue
		}

		if inverted.HasColumnarIndex(prop) {
			eg.Go(func() error {
				return s.createPropertyColumnarIndex(ctx, &propCopy, makeBucketOptions)
			})
		}

		if !inverted.HasAnyInvertedIndex(prop) {
			continue
		}

		eg.Go(func() error {
			if err := s.createPropertyValueIndex(ctx, &propCopy, makeBucketOptions); err != nil {
				return fmt.Errorf("init prop %q: value index: %w", propCopy.Name, err)
			}
			return nil
		})

		if s.index.invertedIndexConfig.IndexNullState {
			eg.Go(func() error {
				if err := s.createPropertyNullIndex(ctx, &propCopy, makeBucketOptions); err != nil {
					return fmt.Errorf("init prop %q: null index: %w", prop.Name, err)
				}
				return nil
			})
		}

		if s.index.invertedIndexConfig.IndexPropertyLength {
			eg.Go(func() error {
				if err := s.createPropertyLengthIndex(ctx, &propCopy, makeBucketOptions); err != nil {
					return fmt.Errorf("init prop %q: length index: %w", prop.Name, err)
				}
				return nil
			})
		}
	}
}

func (s *Shard) updatePropertyBuckets(ctx context.Context,
	eg *enterrors.ErrorGroupWrapper,
	prop *models.Property,
) {
	eg.Go(func() error {
		if !inverted.HasFilterableIndex(prop) {
			mainBucket := helpers.BucketFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove filterable index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "filterable")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		if !inverted.HasSearchableIndex(prop) {
			mainBucket := helpers.BucketSearchableFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove searchable index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "searchable")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		if !inverted.HasRangeableIndex(prop) {
			mainBucket := helpers.BucketRangeableFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove rangeable index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "rangeable")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		if !inverted.HasColumnarIndex(prop) {
			mainBucket := helpers.BucketColumnarFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove columnar index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "columnar")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		return nil
	})
}

// cleanStaleMigrationDirs removes the per-property runtime-reindex
// migration directories whose tidied sentinel would lie now that the
// (propName, indexType) bucket has been removed. Without this, a
// subsequent re-enable of the same index would short-circuit on the
// stale sentinel, re-flip the schema flag to true, and report success
// while leaving the underlying bucket empty.
//
// Errors are logged but not propagated: the bucket has already been
// removed by the time we get here, so the user's DELETE has succeeded
// at the only level that matters for correctness. A failure here only
// affects the next re-enable, which will trigger the defense-in-depth
// check in OnAfterLsmInitAsync and fail with a clear operator error.
func (s *Shard) cleanStaleMigrationDirs(propName, indexType string) {
	cleanStaleMigrationDirsAt(s.pathLSM(), propName, indexType, s.index.logger)
}

// cleanStaleMigrationDirsAt is the pure-function form of
// [Shard.cleanStaleMigrationDirs]: takes an explicit lsmPath + logger so the
// preservation logic can be unit-tested without standing up a Shard.
//
// Every migration tracker dir on disk carries a per-node generation
// suffix (`_<N>`); a single (prop, indexType) tuple can have multiple
// generations on disk simultaneously when the last migration's trim
// hasn't run (e.g. crash before markTidied → next-restart finalize
// cleans up everything). Match by prefix and walk every entry so we
// don't miss old generations.
//
// Tracker dirs with tidied.mig / merged.mig are PRESERVED — they are
// live deferred-finalize state for a successfully completed migration,
// NOT stale partial state. Wiping them out from under the in-memory
// bucket pointer is what produces the #10675-shape silent data loss on
// back-to-back submits without a restart (R2/R2b on the controller
// node).
func cleanStaleMigrationDirsAt(lsmPath, propName, indexType string, logger logrus.FieldLogger) {
	migrationsRoot := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.WithField("path", migrationsRoot).
				Error(fmt.Errorf("read migrations dir for stale-state cleanup: %w", err))
		}
		return
	}
	prefixes := migrationDirsForPropertyIndex(propName, indexType)
	preserved := completedMigrationGens(lsmPath, prefixes)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		matches := false
		for _, p := range prefixes {
			if name == p || strings.HasPrefix(name, p+"_") {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		if _, gen, ok := parseMigrationDirName(name); ok && preserved[gen] {
			logger.WithField("path", filepath.Join(migrationsRoot, name)).
				WithField("gen", gen).
				Info("partial-reindex cleanup: preserving deferred-finalize tracker dir (tidied/merged present)")
			continue
		}
		path := filepath.Join(migrationsRoot, name)
		if err := os.RemoveAll(path); err != nil {
			logger.WithField("path", path).
				Error(fmt.Errorf("failed to clean up stale migration directory after index DELETE: %w; subsequent re-enable will fail loudly via the stale-sentinel check until this directory is removed manually", err))
		}
	}
}

// CleanStalePartialReindexState removes the on-disk state of a previously
// cancelled (or otherwise abandoned) runtime-reindex for the named
// (propName, indexType) on this shard. Mirrors the DELETE-handler cleanup
// (updatePropertyBuckets) on the CANCEL→retry axis: after a cancel, the
// next submit must start from a clean slate, otherwise the retry sees the
// stale started.mig + partial __reindex/__ingest sidecars from the
// cancelled run, short-circuits the iteration to a 50-entry no-op, flips
// the schema flag, and reports success against an empty-or-partial bucket.
// Same Sev 1 family as the DELETE-then-re-enable silent failure fixed in
// 6b7dc23768; structurally distinct because cancel does not remove the
// main bucket.
//
// Three side effects, in order — the order matters for crash safety:
//
//  1. Sidecar buckets (__reindex / __ingest with the per-prop suffix) are
//     shut down if currently loaded in the store. We cannot rm-rf a
//     bucket while the lsmkv layer still has open file handles into it.
//
//  2. Sidecar directories on disk are removed.
//
//  3. The .migrations/<dir>/ for this (prop, indexType) tuple is removed —
//     all sentinel files (started.mig, progress.mig, ...) and the
//     payload.mig recovery record vanish in one call.
//
// Errors at steps 2/3 are logged but not propagated: the caller (cancel
// handler / submit handler) cannot meaningfully recover, and the defense
// in depth in OnAfterLsmInitAsync (stale-tidied-sentinel check) will
// still fail loudly rather than silently report success if a partial
// directory survives. Step 1 errors ARE propagated because they indicate
// a bucket can't be cleanly disconnected from the LSM layer — proceeding
// to remove its files would corrupt the store.
func (s *Shard) CleanStalePartialReindexState(ctx context.Context, propName, indexType string) error {
	// Step 1: shut down the per-prop sidecar buckets for this index type.
	// Only the buckets that share the relevant main bucket's prefix are
	// touched, so other in-flight reindex tasks on the same shard are not
	// disturbed.
	mainBucketName, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return fmt.Errorf("clean stale partial reindex state: unknown indexType %q", indexType)
	}

	logger := s.index.logger.WithFields(map[string]any{
		"shard":       s.Name(),
		"property":    propName,
		"index_type":  indexType,
		"main_bucket": mainBucketName,
		"operation":   "CleanStalePartialReindexState",
	})

	// Compute the set of completed-but-deferred migration generations for
	// this (prop, indexType). Any tracker dir with tidied.mig or merged.mig
	// represents a successfully finished in-process migration whose ingest
	// sidecar dir is the live backing store of the in-memory main bucket
	// pointer — wiping it here is the #10675-shape silent data loss this
	// gate exists to prevent (R2/R2b on the controller node).
	preservePrefixes := migrationDirsForPropertyIndex(propName, indexType)
	preserveGens := completedMigrationGens(s.pathLSM(), preservePrefixes)

	prefix := mainBucketName + "__"
	loaded := s.store.GetBucketsByName()
	var shutDown []string
	for bucketName := range loaded {
		if !strings.HasPrefix(bucketName, prefix) {
			continue
		}
		// Defensive: never shut down the main bucket itself. mainBucketName
		// is the exact name, prefix is mainBucketName+"__" — but a future
		// helper that uses underscores differently could break this; keep
		// the guard.
		if bucketName == mainBucketName {
			continue
		}
		// Skip live sidecar buckets that back a completed-but-deferred
		// migration (in-memory bucket pointer still references them; their
		// dir on disk is the canonical bucket's data until next-restart
		// finalize renames it).
		if len(preserveGens) > 0 {
			if _, gen, ok := parseMigrationDirName(bucketName); ok && preserveGens[gen] {
				continue
			}
		}
		if err := s.store.ShutdownBucket(ctx, bucketName); err != nil {
			if errors.Is(err, lsmkv.ErrBucketNotFound) {
				// Race with another teardown path (in-flight task's own
				// cancel sidecar shutdown, or restart-bootstrap pre-mark)
				// that already removed this bucket. The desired post-state
				// — bucket gone — is satisfied; keep going.
				continue
			}
			return fmt.Errorf(
				"shutting down stale sidecar bucket %q before partial-reindex cleanup: %w",
				bucketName, err)
		}
		shutDown = append(shutDown, bucketName)
	}
	logger.WithField("buckets_shut_down", shutDown).
		WithField("preserved_gens", preserveGensSlice(preserveGens)).
		Info("partial-reindex cleanup: sidecar buckets shut down")

	// Step 2 + 3: remove the sidecar dirs and migration dir. Both call the
	// existing helpers, which log errors rather than panic. Pass through
	// the preserved gens so the deferred-finalize state survives.
	s.cleanStaleSidecarDirsWithPreserved(mainBucketName, preserveGens)
	s.cleanStaleMigrationDirs(propName, indexType)
	logger.Info("partial-reindex cleanup: sidecar dirs + migration dir cleaned")

	return nil
}

// preserveGensSlice flattens a preserved-gens set into a sorted []int for
// stable structured-log output.
func preserveGensSlice(preserveGens map[int]bool) []int {
	out := make([]int, 0, len(preserveGens))
	for g := range preserveGens {
		out = append(out, g)
	}
	sort.Ints(out)
	return out
}

// mainBucketForPropertyIndex returns the canonical main bucket name on
// disk for a given (propName, indexType). Used by
// CleanStalePartialReindexState to compute the prefix that identifies
// per-property sidecar buckets.
func mainBucketForPropertyIndex(propName, indexType string) (string, bool) {
	switch indexType {
	case "filterable":
		return helpers.BucketFromPropNameLSM(propName), true
	case "searchable":
		return helpers.BucketSearchableFromPropNameLSM(propName), true
	case "rangeable":
		return helpers.BucketRangeableFromPropNameLSM(propName), true
	case "columnar":
		return helpers.BucketColumnarFromPropNameLSM(propName), true
	}
	return "", false
}

// cleanStaleSidecarDirs removes leftover __reindex / __ingest / __backup
// sidecar directories that share the just-removed bucket's name as their
// prefix. A successful migration moves the new data into the main bucket
// dir at runtime but defers the actual filesystem renames (old-main ->
// __backup, ingest-dir cleanup) to OnBeforeLsmInit on the next restart.
// Between completion and restart these sidecars live on disk; a DELETE
// then re-enable in the same process lifetime would otherwise hit
// "rename: file exists" the next time RunSwapOnShard tries to move the
// fresh main into __backup.
//
// The sidecar names are <mainBucket>__<strategy-specific-suffix>, where
// suffixes vary per strategy. Matching by prefix avoids hard-coding every
// strategy's suffixes here and naturally absorbs future strategies.
//
// In addition to removing the on-disk dirs, this function ALSO drops the
// dir's entry from [lsmkv.GlobalBucketRegistry]. Background: a successful
// runtime swap moves the in-memory bucket pointer from the ingest name to
// the main name (Store.SwapBucketPointer), but leaves the on-disk dir
// under the ingest name (the dir is renamed by OnBeforeLsmInit on the next
// restart) AND leaves the registry entry under the ingest dir path
// (Bucket.Shutdown is never called on the live ingest bucket — it just
// becomes the main bucket). When a follow-up migration tries to load a
// fresh ingest bucket at the same path, NewBucket's TryAdd fails with
// "bucket already registered" and the migration fails. Removing the
// registry entry alongside the dir keeps the two stores of truth aligned.
// This is the same Sev 1 family as the DELETE-handler cleanup (which has
// the same hazard if any ShutdownBucket call along the way was skipped);
// belt-and-suspenders is the right posture for a leak that produces
// "FAILED" status on a follow-up migration with no clear remediation.
func (s *Shard) cleanStaleSidecarDirs(mainBucketName string) {
	s.cleanStaleSidecarDirsWithPreserved(mainBucketName, nil)
}

// cleanStaleSidecarDirsWithPreserved is the gen-aware variant used by the
// CANCEL→retry and pre-submit defense-in-depth paths. `preserveGens` lists
// the generations whose sidecar dirs MUST be kept on disk because they
// belong to a completed-but-deferred migration (their tracker dir has
// tidied.mig / merged.mig). Wiping those out from under the in-memory
// bucket pointer produces #10675-shape silent data loss on the next
// submit-without-restart sequence.
//
// Pass nil to wipe every matching sidecar dir (the DELETE→re-enable
// callers, which already removed the main bucket and have no live state
// to protect).
func (s *Shard) cleanStaleSidecarDirsWithPreserved(mainBucketName string, preserveGens map[int]bool) {
	entries, err := os.ReadDir(s.pathLSM())
	if err != nil {
		s.index.logger.WithField("path", s.pathLSM()).
			Error(fmt.Errorf("failed to enumerate LSM dir for sidecar cleanup after DELETE: %w; a subsequent re-enable may fail with 'file exists' when RunSwapOnShard tries to rotate buckets", err))
		return
	}
	prefix := mainBucketName + "__"
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if len(preserveGens) > 0 {
			if _, gen, ok := parseMigrationDirName(entry.Name()); ok && preserveGens[gen] {
				s.index.logger.WithField("path", filepath.Join(s.pathLSM(), entry.Name())).
					WithField("gen", gen).
					Info("partial-reindex cleanup: preserving deferred-finalize sidecar dir (live bucket pointer)")
				continue
			}
		}
		path := filepath.Join(s.pathLSM(), entry.Name())
		// Drop the registry entry BEFORE removing the dir. The reverse order
		// is also correct (registry is a separate process-local store), but
		// the chosen order matches Bucket.Shutdown's defer (registry.Remove
		// fires before any other shutdown side effect), so a future reader
		// who already knows that contract finds the same shape here. Either
		// step is independently safe to retry / call when no entry exists.
		lsmkv.GlobalBucketRegistry.Remove(path)
		if err := os.RemoveAll(path); err != nil {
			s.index.logger.WithField("path", path).
				Error(fmt.Errorf("failed to remove stale sidecar bucket dir after index DELETE: %w", err))
		}
	}
}

func (s *Shard) removeBucket(ctx context.Context, bucketName string) error {
	bucket := s.store.Bucket(bucketName)
	if bucket == nil {
		return nil // bucket doesn't exist, nothing to remove
	}
	// Shutdown the bucket first - after this point, the bucket cannot be used
	if err := s.store.ShutdownBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("failed to shutdown bucket %s: %w", bucketName, err)
	}
	// Remove the bucket's directory from disk
	// If this fails after successful shutdown, we're in an inconsistent state:
	// the bucket is removed from the store but its data remains on disk
	if err := s.removeDirIfExists(s.pathLSM(), bucketName); err != nil {
		return fmt.Errorf("bucket %s shut down successfully but directory removal failed: %w", bucketName, err)
	}
	return nil
}

func (s *Shard) removeDirIfExists(parentDir, dirName string) error {
	dirPath := filepath.Join(parentDir, dirName)
	if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("failed to remove data for %s: "+
				"orphaned data remains at %s (manual cleanup may be required): %w", dirName, dirPath, err)
		}
	}
	return nil
}

func (s *Shard) createPropertyValueIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	if inverted.HasFilterableIndex(prop) {
		if dt, _ := schema.AsPrimitive(prop.DataType); dt == schema.DataTypeGeoCoordinates {
			return s.initGeoProp(prop)
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := s.store.CreateOrLoadBucket(ctx,
				helpers.BucketFromPropNameMetaCountLSM(prop.Name),
				makeBucketOptions(lsmkv.StrategyRoaringSet)...,
			); err != nil {
				return err
			}
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketFromPropNameLSM(prop.Name),
			makeBucketOptions(lsmkv.StrategyRoaringSet)...,
		); err != nil {
			return err
		}
	}

	if inverted.HasSearchableIndex(prop) {
		strategy := lsmkv.DefaultSearchableStrategy(s.usingBlockMaxWAND)
		searchableBucketOpts := makeBucketOptions(strategy)

		if s.class.InvertedIndexConfig != nil {
			searchableBucketOpts = append(searchableBucketOpts, lsmkv.WithBM25Config(s.class.InvertedIndexConfig.Bm25))
		}

		bucketName := helpers.BucketSearchableFromPropNameLSM(prop.Name)
		if err := s.store.CreateOrLoadBucket(ctx, bucketName, searchableBucketOpts...); err != nil {
			return err
		}

		if actualStrategy := s.store.Bucket(bucketName).Strategy(); actualStrategy == lsmkv.StrategyInverted {
			s.markSearchableBlockmaxProperties(prop.Name)
		}
	}

	if inverted.HasRangeableIndex(prop) {
		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketRangeableFromPropNameLSM(prop.Name),
			makeBucketOptions(lsmkv.StrategyRoaringSetRange)...,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) createPropertyColumnarIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	colSchema := s.columnarSchemaForProp(prop)
	opts := makeBucketOptions(lsmkv.StrategyColumnar)
	if colSchema != nil {
		opts = append(opts, lsmkv.WithColumnarSchema(colSchema))
	}
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketColumnarFromPropNameLSM(prop.Name),
		opts...,
	)
}

func (s *Shard) createPropertyLengthIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	// some datatypes are not added to the inverted index, so we can skip them here
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeBlobHash, schema.DataTypeInt,
		schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return nil
	default:
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLengthLSM(prop.Name),
		makeBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) createPropertyNullIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameNullLSM(prop.Name),
		makeBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) addIDProperty(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	err := s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropID),
		s.makeDefaultBucketOptions(lsmkv.StrategySetCollection)...,
	)
	if err != nil {
		return fmt.Errorf("create id property: %w", err)
	}
	return nil
}

func (s *Shard) createDimensionsBucket(ctx context.Context, name string) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	bucketPath := filepath.Join(s.pathLSM(), name)
	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	if err != nil {
		return fmt.Errorf("determine dimensions bucket strategy: %w", err)
	}

	if err = s.store.CreateOrLoadBucket(ctx, name, s.makeDefaultBucketOptions(strategy)...); err != nil {
		return fmt.Errorf("create dimensions bucket: %w", err)
	}
	return nil
}

func (s *Shard) addDimensionsProperty(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	// Note: this data would fit the "Set" type better, but since the "Map" type
	// is currently optimized better, it is more efficient to use a Map here.
	err := s.createDimensionsBucket(ctx, helpers.DimensionsBucketLSM)
	if err != nil {
		return fmt.Errorf("create dimensions tracking property: %w", err)
	}

	return nil
}

func (s *Shard) addTimestampProperties(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	if err := s.addCreationTimeUnixProperty(ctx); err != nil {
		return fmt.Errorf("create creation time property: %w", err)
	}

	if err := s.addLastUpdateTimeUnixProperty(ctx); err != nil {
		return fmt.Errorf("create last update time property: %w", err)
	}

	return nil
}

func (s *Shard) addCreationTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropCreationTimeUnix),
		s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) addLastUpdateTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) markSearchableBlockmaxProperties(propNames ...string) {
	s.searchableBlockmaxPropNamesLock.Lock()
	s.searchableBlockmaxPropNames = append(s.searchableBlockmaxPropNames, propNames...)
	s.searchableBlockmaxPropNamesLock.Unlock()
}

func (s *Shard) getSearchableBlockmaxProperties() []string {
	// since slice is only appended, it should be safe to return it that way
	s.searchableBlockmaxPropNamesLock.Lock()
	defer s.searchableBlockmaxPropNamesLock.Unlock()
	return s.searchableBlockmaxPropNames
}

// columnarSchemaForProp returns the columnar schema for a single property.
// The schema has a single column matching the property's data type.
func (s *Shard) columnarSchemaForProp(prop *models.Property) *columnar.Schema {
	dt, _ := schema.AsPrimitive(prop.DataType)
	var colType columnar.ColumnType
	switch dt {
	case schema.DataTypeInt, schema.DataTypeDate:
		colType = columnar.ColumnTypeInt64
	case schema.DataTypeNumber:
		colType = columnar.ColumnTypeFloat64
	default:
		return nil
	}
	return &columnar.Schema{
		Columns: []columnar.Column{
			{Name: prop.Name, Type: colType},
		},
	}
}

// columnarSchemaForPropName resolves the property by name from the shard's
// class and returns its columnar schema. Returns nil when the property is
// unknown or its data type has no columnar representation. Used by the
// enable-columnar migration, which only knows property names.
func (s *Shard) columnarSchemaForPropName(propName string) *columnar.Schema {
	if s.class == nil {
		return nil
	}
	for _, p := range s.class.Properties {
		if p.Name == propName {
			return s.columnarSchemaForProp(p)
		}
	}
	return nil
}
