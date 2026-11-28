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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/errorcompounder"
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

	initGeoIndex := func(prop *models.Property) error {
		if err := s.isReadOnly(); err != nil {
			return fmt.Errorf("init prop %q: geo index: %w", prop.Name, err)
		}
		if err := s.initGeoProp(prop); err != nil {
			return fmt.Errorf("init prop %q: geo index: %w", prop.Name, err)
		}
		return nil
	}

	// when the server waits for cache prefill (the default), each geo prop's init
	// blocks on a 2*GOMAXPROCS-cursor scan of the objects bucket, so they run one
	// at a time rather than joining the fan-out below
	var geoProps []*models.Property

	for _, prop := range props {
		prop = s.withViewIndexes(prop)
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

		if createsGeoIndex(prop) {
			geoProps = append(geoProps, &propCopy)
		}
		// one job per index family, as before: value buckets, null, length
		var value, null, length []bucketNeed
		for _, need := range s.propertyBucketNeeds(prop, s.loadView().props[prop.Name].gen, makeBucketOptions) {
			switch need.kind {
			case bucketNeedNull:
				null = append(null, need)
			case bucketNeedLength:
				length = append(length, need)
			default:
				value = append(value, need)
			}
		}
		for _, job := range []struct {
			family string
			needs  []bucketNeed
		}{{"value", value}, {"null", null}, {"length", length}} {
			if len(job.needs) == 0 {
				continue
			}
			eg.Go(func() error {
				for _, need := range job.needs {
					if err := s.openBucketNeed(ctx, need); err != nil {
						return fmt.Errorf("init prop %q: %s index: %w", prop.Name, job.family, err)
					}
				}
				return nil
			})
		}
	}

	if len(geoProps) > 0 {
		eg.Go(func() error {
			// one prop failing must not skip the rest, which each own a separate
			// index the shard would otherwise come up without
			ec := errorcompounder.New()
			for _, prop := range geoProps {
				if err := ctx.Err(); err != nil {
					ec.Add(err)
					break
				}
				ec.Add(initGeoIndex(prop))
			}
			return ec.ToError()
		})
	}
}

// createsGeoIndex reports whether createPropertyValueIndex builds a geo index
// for prop rather than inverted buckets.
func createsGeoIndex(prop *models.Property) bool {
	if !inverted.HasFilterableIndex(prop) {
		return false
	}
	dt, _ := schema.AsPrimitive(prop.DataType)
	return dt == schema.DataTypeGeoCoordinates
}

// sidecarRoleWords are the words every migration sidecar suffix ends in, once
// the generation tail is off, plus "backup"/"map" that only upgrades bring in.
var sidecarRoleWords = schema.SidecarRoleWords

// isSidecarDirOf reports whether name is a per-property sidecar of
// mainBucketName. "__" alone isn't enough: property names may contain "__"
// too, so "property_a__b" is "a__b"'s own main bucket, not a sidecar of "a"
// — the trailing role word decides instead.
//
// Still too weak: a property named "a__<word>_<role>" (or "a___del") reads as
// a sidecar of "a" on all three index types, so sweeping "a" deletes that
// property's live bucket. weaviate/weaviate#12621
func isSidecarDirOf(name, mainBucketName string) bool {
	if name == mainBucketName+lsmkv.ReplacedBucketDirSuffix {
		return true
	}
	suffix, ok := strings.CutPrefix(name, mainBucketName+"__")
	if !ok {
		return false
	}
	return slices.Contains(sidecarRoleWords, sidecarRoleWord(suffix))
}

// sidecarRoleWord returns a sidecar suffix's trailing word, ignoring the
// "_<gen>" tail [genSuffix] appends. Shared with the property-name check that
// refuses a name of this shape at creation time.
func sidecarRoleWord(suffix string) string {
	return schema.SidecarRoleWord(suffix)
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

const (
	bucketNeedMetaCount = "ref count"
	bucketNeedNull      = "null"
	bucketNeedLength    = "length"
)

type bucketNeed struct {
	kind string // an index type or a bucketNeed* constant
	name string
	opts []lsmkv.BucketOption
}

// propertyBucketNeeds lists the buckets a load opens for prop, served at gen,
// so a refresh opens the same set. Geo and nested indexes open in the load only.
func (s *Shard) propertyBucketNeeds(prop *models.Property, gen map[string]uint64,
	makeBucketOptions lsmkv.MakeBucketOptions,
) []bucketNeed {
	if _, nested := schema.AsNested(prop.DataType); nested || !inverted.HasAnyInvertedIndex(prop) {
		return nil
	}
	var needs []bucketNeed
	add := func(kind, name string, opts []lsmkv.BucketOption) {
		needs = append(needs, bucketNeed{kind: kind, name: name, opts: opts})
	}
	if !createsGeoIndex(prop) {
		if inverted.HasFilterableIndex(prop) && schema.IsRefDataType(prop.DataType) {
			add(bucketNeedMetaCount, helpers.BucketFromPropNameMetaCountLSM(prop.Name), makeBucketOptions(lsmkv.StrategyRoaringSet))
		}
		for _, t := range reindexIndexTypes {
			if classIndexOn(prop, t) {
				add(t, reindexBucketName(prop.Name, t, gen[t]), s.propertyIndexOptions(prop, t, makeBucketOptions))
			}
		}
	}
	if s.index.invertedIndexConfig.IndexNullState {
		add(bucketNeedNull, helpers.BucketFromPropNameNullLSM(prop.Name), makeBucketOptions(lsmkv.StrategyRoaringSet))
	}
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeBlobHash, schema.DataTypeInt,
		schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate: // not in the inverted index
	default:
		if s.index.invertedIndexConfig.IndexPropertyLength {
			add(bucketNeedLength, helpers.BucketFromPropNameLengthLSM(prop.Name), makeBucketOptions(lsmkv.StrategyRoaringSet))
		}
	}
	return needs
}

func (s *Shard) openBucketNeed(ctx context.Context, need bucketNeed) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}
	return s.store.CreateOrLoadBucket(ctx, need.name, need.opts...)
}

func (s *Shard) propertyIndexOptions(prop *models.Property, indexType string,
	makeBucketOptions lsmkv.MakeBucketOptions,
) []lsmkv.BucketOption {
	switch indexType {
	case indexTypeSearchable:
		// The per-property stamp overrides the class-level flag: a new tenant
		// shard on a permanently-partial class must create a blockmax bucket for
		// an already-stamped property, not default to the class-wide WAND flag.
		blockmax := s.usingBlockMaxWAND
		if prop.SearchableBlockmax != nil {
			blockmax = *prop.SearchableBlockmax
		}
		opts := makeBucketOptions(lsmkv.DefaultSearchableStrategy(blockmax))
		if s.class.InvertedIndexConfig != nil {
			opts = append(opts, lsmkv.WithBM25Config(s.class.InvertedIndexConfig.Bm25))
		}
		return opts
	case indexTypeRangeable:
		// Appended last so it wins over makeDefaultBucketOptions' value.
		return append(makeBucketOptions(lsmkv.StrategyRoaringSetRange),
			lsmkv.WithKeepSegmentsInMemory(s.index.Config.keepRangeableInMemory(prop.Name)))
	default:
		return makeBucketOptions(lsmkv.StrategyRoaringSet)
	}
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
	return s.loadDimensionsBucket(ctx, name)
}

// loadDimensionsBucket, unlike createDimensionsBucket, does not refuse a read only shard.
func (s *Shard) loadDimensionsBucket(ctx context.Context, name string) error {
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

	// a usage scan that takes the shard for unloaded must not open the bucket meanwhile
	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, s.index.path(), s.name)
	if err != nil {
		return fmt.Errorf("create dimensions tracking property: %w", err)
	}
	defer unlock()

	// nothing can read or write the bucket yet, so a migration needs no double writes
	if _, err := shardusage.PrepareDimensionsBucket(ctx, s.index.logger, s.index.path(), s.name,
		s.index.Config.MigrateDimensionsToRoaringSet); err != nil {
		return fmt.Errorf("create dimensions tracking property: %w", err)
	}

	if err := s.createDimensionsBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
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
