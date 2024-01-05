//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

type ShardInvertedReindexTask interface {
	GetPropertiesToReindex(ctx context.Context, shard ShardLike,
	) ([]ReindexableProperty, error)
	// right now only OnResume is needed, but in the future more
	// callbacks could be added
	// (like OnPrePauseStore, OnPostPauseStore, OnPreResumeStore, etc)
	OnPostResumeStore(ctx context.Context, shard ShardLike) error
}

type ReindexableProperty struct {
	PropertyName    string
	IndexType       PropertyIndexType
	NewIndex        bool // is new index, there is no bucket to replace with
	DesiredStrategy string
	BucketOptions   []lsmkv.BucketOption
}

type ShardInvertedReindexer struct {
	logger logrus.FieldLogger
	shard  ShardLike

	tasks []ShardInvertedReindexTask
	class *models.Class
}

func NewShardInvertedReindexer(shard ShardLike, logger logrus.FieldLogger) *ShardInvertedReindexer {
	class, _ := schema.GetClassByName(shard.Index().getSchema.GetSchemaSkipAuth().Objects,
		shard.Index().Config.ClassName.String())

	return &ShardInvertedReindexer{
		logger: logger,
		shard:  shard,
		tasks:  []ShardInvertedReindexTask{},
		class:  class,
	}
}

func (r *ShardInvertedReindexer) AddTask(task ShardInvertedReindexTask) {
	r.tasks = append(r.tasks, task)
}

func (r *ShardInvertedReindexer) Do(ctx context.Context) error {
	for _, task := range r.tasks {
		if err := r.checkContextExpired(ctx, "remaining tasks skipped due to context canceled"); err != nil {
			return err
		}
		if err := r.doTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func (r *ShardInvertedReindexer) doTask(ctx context.Context, task ShardInvertedReindexTask) error {
	reindexProperties, err := task.GetPropertiesToReindex(ctx, r.shard)
	if err != nil {
		r.logError(err, "failed getting reindex properties")
		return errors.Wrapf(err, "failed getting reindex properties")
	}
	if len(reindexProperties) == 0 {
		r.logger.
			WithField("action", "inverted reindex").
			WithField("index", r.shard.Index().ID()).
			WithField("shard", r.shard.ID()).
			Debug("no properties to reindex")
		return nil
	}

	if err := r.checkContextExpired(ctx, "pausing store stopped due to context canceled"); err != nil {
		return err
	}

	if err := r.pauseStoreActivity(ctx); err != nil {
		r.logError(err, "failed pausing store activity")
		return err
	}

	bucketsToReindex := make([]string, len(reindexProperties))
	for i, reindexProperty := range reindexProperties {
		if err := r.checkContextExpired(ctx, "creating temp buckets stopped due to context canceled"); err != nil {
			return err
		}

		if !isIndexTypeSupportedByStrategy(reindexProperty.IndexType, reindexProperty.DesiredStrategy) {
			err := fmt.Errorf("strategy '%s' is not supported for given index type '%d",
				reindexProperty.DesiredStrategy, reindexProperty.IndexType)
			r.logError(err, "invalid strategy")
			return err
		}

		// TODO verify if property indeed need reindex before creating buckets
		// (is filterable / is searchable / null or prop length index enabled)
		bucketsToReindex[i] = r.bucketName(reindexProperty.PropertyName, reindexProperty.IndexType)
		if err := r.createTempBucket(ctx, bucketsToReindex[i], reindexProperty.DesiredStrategy,
			reindexProperty.BucketOptions...); err != nil {
			r.logError(err, "failed creating temporary bucket")
			return err
		}
		r.logger.
			WithField("action", "inverted reindex").
			WithField("shard", r.shard.Name()).
			WithField("property", reindexProperty.PropertyName).
			WithField("strategy", reindexProperty.DesiredStrategy).
			WithField("index_type", reindexProperty.IndexType).
			Debug("created temporary bucket")
	}

	if err := r.reindexProperties(ctx, reindexProperties); err != nil {
		r.logError(err, "failed reindexing properties")
		return errors.Wrapf(err, "failed reindexing properties on shard '%s'", r.shard.Name())
	}

	for i := range bucketsToReindex {
		if err := r.checkContextExpired(ctx, "replacing buckets stopped due to context canceled"); err != nil {
			return err
		}
		tempBucketName := helpers.TempBucketFromBucketName(bucketsToReindex[i])
		tempBucket := r.shard.Store().Bucket(tempBucketName)
		tempBucket.FlushMemtable()
		tempBucket.UpdateStatus(storagestate.StatusReadOnly)

		if reindexProperties[i].NewIndex {
			if err := r.shard.Store().RenameBucket(ctx, tempBucketName, bucketsToReindex[i]); err != nil {
				r.logError(err, "failed renaming buckets")
				return err
			}

			r.logger.
				WithField("action", "inverted reindex").
				WithField("shard", r.shard.Name()).
				WithField("bucket", bucketsToReindex[i]).
				WithField("temp_bucket", tempBucketName).
				Debug("renamed bucket")
		} else {
			if err := r.shard.Store().ReplaceBuckets(ctx, bucketsToReindex[i], tempBucketName); err != nil {
				r.logError(err, "failed replacing buckets")
				return err
			}

			r.logger.
				WithField("action", "inverted reindex").
				WithField("shard", r.shard.Name()).
				WithField("bucket", bucketsToReindex[i]).
				WithField("temp_bucket", tempBucketName).
				Debug("replaced buckets")
		}
	}

	if err := r.checkContextExpired(ctx, "resuming store stopped due to context canceled"); err != nil {
		return err
	}

	if err := r.resumeStoreActivity(ctx, task); err != nil {
		r.logError(err, "failed resuming store activity")
		return err
	}

	return nil
}

func (r *ShardInvertedReindexer) pauseStoreActivity(ctx context.Context) error {
	if err := r.shard.Store().PauseCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed pausing compaction for shard '%s'", r.shard.Name())
	}
	if err := r.shard.Store().FlushMemtables(ctx); err != nil {
		return errors.Wrapf(err, "failed flushing memtables for shard '%s'", r.shard.Name())
	}
	r.shard.Store().UpdateBucketsStatus(storagestate.StatusReadOnly)

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.Name()).
		Debug("paused store activity")

	return nil
}

func (r *ShardInvertedReindexer) resumeStoreActivity(ctx context.Context, task ShardInvertedReindexTask) error {
	if err := r.shard.Store().ResumeCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed resuming compaction for shard '%s'", r.shard.Name())
	}
	r.shard.Store().UpdateBucketsStatus(storagestate.StatusReady)
	if err := task.OnPostResumeStore(ctx, r.shard); err != nil {
		return errors.Wrap(err, "failed OnPostResumeStore")
	}

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.Name()).
		Debug("resumed store activity")

	return nil
}

func (r *ShardInvertedReindexer) createTempBucket(ctx context.Context, name string,
	strategy string, options ...lsmkv.BucketOption,
) error {
	tempName := helpers.TempBucketFromBucketName(name)
	bucketOptions := append(options, lsmkv.WithStrategy(strategy))

	if err := r.shard.Store().CreateBucket(ctx, tempName, bucketOptions...); err != nil {
		return errors.Wrapf(err, "failed creating temp bucket '%s'", tempName)
	}
	return nil
}

func (r *ShardInvertedReindexer) reindexProperties(ctx context.Context, reindexableProperties []ReindexableProperty) error {
	checker := newReindexablePropertyChecker(reindexableProperties, r.class)
	objectsBucket := r.shard.Store().Bucket(helpers.ObjectsBucketLSM)

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.Name()).
		Debug("starting populating indexes")

	i := 0
	if err := objectsBucket.IterateObjects(ctx, func(object *storobj.Object) error {
		// check context expired every 100k objects
		if i%100_000 == 0 && i != 0 {
			if err := r.checkContextExpired(ctx, "iterating through objects stopped due to context canceled"); err != nil {
				return err
			}
			r.logger.
				WithField("action", "inverted reindex").
				WithField("shard", r.shard.Name()).
				Debugf("iterating through objects: %d done", i)
		}
		docID := object.DocID()
		properties, nilProperties, err := r.shard.AnalyzeObject(object)
		if err != nil {
			return errors.Wrapf(err, "failed analyzying object")
		}

		for _, property := range properties {
			if err := r.handleProperty(ctx, checker, docID, property); err != nil {
				return errors.Wrapf(err, "failed reindexing property '%s' of object '%d'", property.Name, docID)
			}
		}
		for _, nilProperty := range nilProperties {
			if err := r.handleNilProperty(ctx, checker, docID, nilProperty); err != nil {
				return errors.Wrapf(err, "failed reindexing property '%s' of object '%d'", nilProperty.Name, docID)
			}
		}

		i++
		return nil
	}); err != nil {
		return err
	}

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.Name()).
		Debugf("iterating through objects: %d done", i)

	return nil
}

func (r *ShardInvertedReindexer) handleProperty(ctx context.Context, checker *reindexablePropertyChecker,
	docID uint64, property inverted.Property,
) error {
	reindexablePropValue := checker.isReindexable(property.Name, IndexTypePropValue)
	reindexablePropSearchableValue := checker.isReindexable(property.Name, IndexTypePropSearchableValue)

	if reindexablePropValue || reindexablePropSearchableValue {
		schemaProp := checker.getSchemaProp(property.Name)

		var bucketValue, bucketSearchableValue *lsmkv.Bucket

		if reindexablePropValue {
			bucketValue = r.tempBucket(property.Name, IndexTypePropValue)
			if bucketValue == nil {
				return fmt.Errorf("no bucket for prop '%s' value found", property.Name)
			}
		}
		if reindexablePropSearchableValue {
			bucketSearchableValue = r.tempBucket(property.Name, IndexTypePropSearchableValue)
			if bucketSearchableValue == nil {
				return fmt.Errorf("no bucket searchable for prop '%s' value found", property.Name)
			}
		}

		propLen := float32(len(property.Items))
		for _, item := range property.Items {
			key := item.Data
			if reindexablePropSearchableValue && inverted.HasSearchableIndex(schemaProp) {
				pair := r.shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
				if err := r.shard.addToPropertyMapBucket(bucketSearchableValue, pair, key); err != nil {
					return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
				}
			}
			if reindexablePropValue && inverted.HasFilterableIndex(schemaProp) {
				if err := r.shard.addToPropertySetBucket(bucketValue, docID, key); err != nil {
					return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
				}
			}
		}
	}

	// add non-nil properties to the null-state inverted index,
	// but skip internal properties (__meta_count, _id etc)
	if isMetaCountProperty(property) || isInternalProperty(property) {
		return nil
	}

	// properties where defining a length does not make sense (floats etc.) have a negative entry as length
	if r.shard.Index().invertedIndexConfig.IndexPropertyLength && property.Length >= 0 {
		key, err := r.shard.keyPropertyLength(property.Length)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' length", property.Name)
		}
		if checker.isReindexable(property.Name, IndexTypePropLength) {
			bucketLength := r.tempBucket(property.Name, IndexTypePropLength)
			if bucketLength == nil {
				return fmt.Errorf("no bucket for prop '%s' length found", property.Name)
			}
			if err := r.shard.addToPropertySetBucket(bucketLength, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' length bucket", property.Name)
			}
		}
	}

	if r.shard.Index().invertedIndexConfig.IndexNullState {
		key, err := r.shard.keyPropertyNull(property.Length == 0)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' null", property.Name)
		}
		if checker.isReindexable(property.Name, IndexTypePropNull) {
			bucketNull := r.tempBucket(property.Name, IndexTypePropNull)
			if bucketNull == nil {
				return fmt.Errorf("no bucket for prop '%s' null found", property.Name)
			}
			if err := r.shard.addToPropertySetBucket(bucketNull, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' null bucket", property.Name)
			}
		}
	}

	return nil
}

func (r *ShardInvertedReindexer) handleNilProperty(ctx context.Context, checker *reindexablePropertyChecker,
	docID uint64, nilProperty nilProp,
) error {
	if r.shard.Index().invertedIndexConfig.IndexPropertyLength && nilProperty.AddToPropertyLength {
		key, err := r.shard.keyPropertyLength(0)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' length", nilProperty.Name)
		}
		if checker.isReindexable(nilProperty.Name, IndexTypePropLength) {
			bucketLength := r.tempBucket(nilProperty.Name, IndexTypePropLength)
			if bucketLength == nil {
				return fmt.Errorf("no bucket for prop '%s' length found", nilProperty.Name)
			}
			if err := r.shard.addToPropertySetBucket(bucketLength, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' length bucket", nilProperty.Name)
			}
		}
	}

	if r.shard.Index().invertedIndexConfig.IndexNullState {
		key, err := r.shard.keyPropertyNull(true)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' null", nilProperty.Name)
		}
		if checker.isReindexable(nilProperty.Name, IndexTypePropNull) {
			bucketNull := r.tempBucket(nilProperty.Name, IndexTypePropNull)
			if bucketNull == nil {
				return fmt.Errorf("no bucket for prop '%s' null found", nilProperty.Name)
			}
			if err := r.shard.addToPropertySetBucket(bucketNull, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' null bucket", nilProperty.Name)
			}
		}
	}

	return nil
}

func (r *ShardInvertedReindexer) bucketName(propName string, indexType PropertyIndexType) string {
	checkSupportedPropertyIndexType(indexType)

	switch indexType {
	case IndexTypePropValue:
		return helpers.BucketFromPropNameLSM(propName)
	case IndexTypePropSearchableValue:
		return helpers.BucketSearchableFromPropNameLSM(propName)
	case IndexTypePropLength:
		return helpers.BucketFromPropNameLengthLSM(propName)
	case IndexTypePropNull:
		return helpers.BucketFromPropNameNullLSM(propName)
	default:
		return ""
	}
}

func (r *ShardInvertedReindexer) tempBucket(propName string, indexType PropertyIndexType) *lsmkv.Bucket {
	tempBucketName := helpers.TempBucketFromBucketName(r.bucketName(propName, indexType))
	return r.shard.Store().Bucket(tempBucketName)
}

func (r *ShardInvertedReindexer) checkContextExpired(ctx context.Context, msg string) error {
	if ctx.Err() != nil {
		r.logError(ctx.Err(), msg)
		return errors.Wrapf(ctx.Err(), msg)
	}
	return nil
}

func (r *ShardInvertedReindexer) logError(err error, msg string, args ...interface{}) {
	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.Name()).
		WithError(err).
		Errorf(msg, args...)
}
