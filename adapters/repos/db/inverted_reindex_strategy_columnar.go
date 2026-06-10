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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/cluster/proto/api"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

// EnableColumnarStrategy implements MigrationStrategy for building columnar
// (per-property column store) buckets from existing objects. It backs the
// "enable-columnar" migration: flipping indexColumnar=true on a property
// that already holds data.
//
// Backfill source. Like FilterableToRangeableStrategy, the reindex iterator
// scans the objects bucket and runs the inverted analyzer to derive
// per-property values. It does NOT read from any other inverted bucket —
// a numeric property may have every other index flag disabled and the
// columnar bucket is still buildable from objects.
//
// Schema-flag gating. During the backfill scan, IndexColumnar is still
// false on the target property until OnMigrationComplete flips it. Without
// an AnalyzerOverlay forcing the columnar flag on, the analyzer would
// either drop the property entirely (when no other inverted index is
// enabled) or emit it with HasColumnarIndex=false. Either way the new
// columnar bucket would end up empty while the task still reports
// FINISHED — the same silent-data-loss class the rangeable strategy's
// matrix pinned.
//
// Bucket schema. Columnar buckets refuse writes until a *columnar.Schema
// is set (lsmkv.WithColumnarSchema). The main bucket gets it in
// PreReindexHook; the reindex/ingest/backup sidecars get it via
// [EnableColumnarStrategy.PerPropertyBucketOptions], the optional
// per-property extension consumed by the generic task's loadBuckets.
type EnableColumnarStrategy struct {
	schemaManager *schema.Manager
	propNames     []string
	generation    int // see genSuffix godoc
}

func (s *EnableColumnarStrategy) MigrationDirName() string {
	return migrationDirWithProps(MigrationDirPrefixEnableColumnar, s.propNames) + genSuffix(s.generation)
}

func (s *EnableColumnarStrategy) SourceBucketName(propName string) string {
	return helpers.BucketColumnarFromPropNameLSM(propName)
}

func (s *EnableColumnarStrategy) ReindexSuffix() string {
	return "__columnar_reindex" + genSuffix(s.generation)
}

func (s *EnableColumnarStrategy) IngestSuffix() string {
	return "__columnar_ingest" + genSuffix(s.generation)
}

func (s *EnableColumnarStrategy) BackupSuffix() string {
	return "__columnar_backup" + genSuffix(s.generation)
}

func (s *EnableColumnarStrategy) SourceStrategy() string {
	return lsmkv.StrategyColumnar
}

func (s *EnableColumnarStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropColumnarValue
}

func (s *EnableColumnarStrategy) TargetStrategy() string {
	return lsmkv.StrategyColumnar
}

func (s *EnableColumnarStrategy) BackupStrategy() string {
	return lsmkv.StrategyColumnar
}

// PerPropertyBucketOptions supplies the per-property columnar schema for
// the reindex/ingest/backup sidecar buckets the generic task creates from
// TargetStrategy/BackupStrategy. Without it, the sidecars come up without
// a columnar schema and every write fails with "columnar schema not set".
// Implements the optional [perPropertyBucketOptioner] extension.
func (s *EnableColumnarStrategy) PerPropertyBucketOptions(shard *Shard, propName string) []lsmkv.BucketOption {
	colSchema := shard.columnarSchemaForPropName(propName)
	if colSchema == nil {
		return nil
	}
	return []lsmkv.BucketOption{lsmkv.WithColumnarSchema(colSchema)}
}

// writeColumnarValue decodes the analyzer's lexicographically sortable
// 8-byte payload per the bucket's column type and writes it to the
// columnar bucket. The column type comes from the bucket's own schema
// (set at creation from the property's data type, see
// [Shard.columnarSchemaForProp]): number → float64, int/date → int64.
// Columnar holds at most one value per (docID, column), so only Items[0]
// is consulted. Shared by the live write path ([Shard.addToColumnarIndex])
// and the migration's backfill/double-write paths.
func writeColumnarValue(bucket *lsmkv.Bucket, docID uint64, prop *inverted.Property) error {
	if len(prop.Items) == 0 {
		return nil
	}
	item := prop.Items[0]
	if len(item.Data) != 8 {
		return fmt.Errorf("columnar prop '%s' docID %d: invalid value length %d, should be 8 bytes",
			prop.Name, docID, len(item.Data))
	}
	colSchema := bucket.ColumnarSchema()
	if colSchema == nil || len(colSchema.Columns) == 0 {
		return fmt.Errorf("columnar prop '%s': bucket has no columnar schema", prop.Name)
	}
	switch colSchema.Columns[0].Type {
	case columnar.ColumnTypeFloat64: // number
		v, err := entinverted.ParseLexicographicallySortableFloat64(item.Data)
		if err != nil {
			return fmt.Errorf("columnar: decode float64 for prop '%s': %w", prop.Name, err)
		}
		return bucket.ColumnarPutFloat64(docID, 0, v)
	default: // ColumnTypeInt64: int, date — both stored as int64
		v, err := entinverted.ParseLexicographicallySortableInt64(item.Data)
		if err != nil {
			return fmt.Errorf("columnar: decode int64 for prop '%s': %w", prop.Name, err)
		}
		return bucket.ColumnarPutInt64(docID, 0, v)
	}
}

func (s *EnableColumnarStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	return writeColumnarValue(bucket, docID, &prop)
}

// ShouldProcessProperty always returns true. Scope is driven by the
// reindexTaskConfig.selectedPropsByCollection set in the task constructor,
// not by the live schema flag — during this migration IndexColumnar is
// still false on every targeted property.
func (s *EnableColumnarStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return true
}

func (s *EnableColumnarStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		// Don't gate on HasColumnarIndex — the schema flag is still false
		// mid-migration. Scope is enforced via propsByName.
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		if bucket == nil {
			return fmt.Errorf("columnar double-write: bucket '%s' not found", bucketName)
		}
		if err := writeColumnarValue(bucket, docID, property); err != nil {
			return fmt.Errorf("adding columnar prop '%s' to bucket '%s': %w",
				property.Name, bucketName, err)
		}
		return nil
	}
}

func (s *EnableColumnarStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		// Don't gate on HasColumnarIndex — see MakeAddCallback.
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		if bucket == nil {
			return fmt.Errorf("columnar double-write: bucket '%s' not found", bucketName)
		}
		if err := bucket.ColumnarDelete(docID); err != nil {
			return fmt.Errorf("deleting columnar prop '%s' docID %d from bucket '%s': %w",
				property.Name, docID, bucketName, err)
		}
		return nil
	}
}

// PreReindexHook creates empty columnar buckets so the swap phase has a
// "source" bucket to replace with the populated ingest bucket.
//
// It also pessimistically marks each migrated property as
// "not locally ready" on this shard. The columnar read paths
// (aggregator's columnarBucketFor, DB.BoostValues) consult this via
// [*Shard.IsColumnarLocallyReady] and fall back to the object path while
// the columnar bucket is empty/partial. The post-runtimeSwap finalize
// (OnMigrationComplete) flips the prop back to "ready" after markTidied.
// Same per-shard ready-flag mechanism as Shard.rangeableLocalReady.
func (s *EnableColumnarStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		shard.setColumnarLocallyReady(propName, false)
		bucketName := helpers.BucketColumnarFromPropNameLSM(propName)
		if shard.store.Bucket(bucketName) != nil {
			continue
		}
		opts := shard.makeDefaultBucketOptions(lsmkv.StrategyColumnar)
		if colSchema := shard.columnarSchemaForPropName(propName); colSchema != nil {
			opts = append(opts, lsmkv.WithColumnarSchema(colSchema))
		}
		if err := shard.store.CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
			shard.index.logger.WithField("bucket", bucketName).
				WithError(err).Error("PreReindexHook: failed to create columnar bucket")
		}
	}
}

// AnalyzerOverlay forces IndexColumnar=true on the targeted properties
// while the backfill iterator scans the objects bucket. Until
// OnMigrationComplete flips the RAFT-stored schema flag, the analyzer
// would otherwise emit the property with HasColumnarIndex=false (and skip
// it entirely when no other inverted index is enabled), leaving the new
// columnar bucket empty — the silent-FINISHED data-loss failure mode.
func (s *EnableColumnarStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]inverted.PropertyOverlay, len(props))
	for _, p := range props {
		out[p] = inverted.PropertyOverlay{ForceColumnar: true}
	}
	return out
}

// OnMigrationComplete updates the schema to set IndexColumnar=true on the
// migrated properties via per-property UpdateProperty RAFT commands with a
// fieldmask, mirroring [FilterableToRangeableStrategy.OnMigrationComplete]
// including its staleness-window handling (the class is re-read right
// before each per-property update inside applyPerPropertySchemaUpdate).
func (s *EnableColumnarStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	// Mark each prop as "locally ready" so the columnar read paths stop
	// falling back to the object path for this shard. This MUST happen
	// before the schema-flag flip below — once the schema flip RAFTs
	// cluster-wide, queries on THIS shard that observe the new schema
	// flag must also see ready=true. Same ordering contract as the
	// rangeable strategy (see its OnMigrationComplete).
	//
	// Unwrap before the assertion: a *LazyLoadShard wraps the concrete
	// *Shard we need to flip the flag on.
	if concrete, err := unwrapShard(ctx, shard); err == nil && concrete != nil {
		for _, propName := range s.propNames {
			concrete.setColumnarLocallyReady(propName, true)
		}
	}

	className := shard.Index().Config.ClassName.String()
	trueVal := true
	// Missing properties are tolerated: a property dropped between
	// scheduling and completion is the same outcome we'd want anyway.
	_, err := applyPerPropertySchemaUpdate(ctx, s.schemaManager, className, s.propNames,
		[]string{api.PropertyFieldIndexColumnar},
		func(prop *models.Property) bool {
			if prop.IndexColumnar != nil && *prop.IndexColumnar {
				return false // already enabled
			}
			prop.IndexColumnar = &trueVal
			return true
		})
	return err
}
