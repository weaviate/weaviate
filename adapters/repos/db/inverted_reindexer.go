//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

type ShardInvertedReindexTask interface {
	GetPropertiesToReindex(ctx context.Context, store *lsmkv.Store, indexConfig IndexConfig,
		invertedIndexConfig schema.InvertedIndexConfig, logger logrus.FieldLogger,
	) ([]ReindexableProperty, error)
}

type ReindexableProperty struct {
	PropertyName    string
	IndexType       PropertyIndexType
	DesiredStrategy string
	BucketOptions   []lsmkv.BucketOption
}

type ShardInvertedReindexer struct {
	logger logrus.FieldLogger
	shard  *Shard

	tasks []ShardInvertedReindexTask
}

func NewShardInvertedReindexer(shard *Shard, logger logrus.FieldLogger) *ShardInvertedReindexer {
	return &ShardInvertedReindexer{logger: logger, shard: shard, tasks: []ShardInvertedReindexTask{}}
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
	reindexProperties, err := task.GetPropertiesToReindex(ctx, r.shard.store,
		r.shard.index.Config, r.shard.index.invertedIndexConfig, r.logger)
	if err != nil {
		r.logError(err, "failed getting reindex properties")
		return errors.Wrapf(err, "failed getting reindex properties")
	}
	if len(reindexProperties) == 0 {
		r.logger.
			WithField("action", "inverted reindex").
			WithField("shard", r.shard.name).
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

		if !IsIndexTypeSupportedByStrategy(reindexProperty.IndexType, reindexProperty.DesiredStrategy) {
			err := fmt.Errorf("strategy '%s' is not supported for given index type '%d",
				reindexProperty.DesiredStrategy, reindexProperty.IndexType)
			r.logError(err, "invalid strategy")
			return err
		}

		bucketsToReindex[i] = r.bucketName(reindexProperty.PropertyName, reindexProperty.IndexType)
		if err := r.createTempBucket(ctx, bucketsToReindex[i], reindexProperty.DesiredStrategy,
			reindexProperty.BucketOptions...); err != nil {
			r.logError(err, "failed creating temporary bucket")
			return err
		}
		r.logger.
			WithField("action", "inverted reindex").
			WithField("shard", r.shard.name).
			WithField("property", reindexProperty.PropertyName).
			WithField("strategy", reindexProperty.DesiredStrategy).
			WithField("index_type", reindexProperty.IndexType).
			Debug("created temporary bucket")
	}

	if err := r.reindexProperties(ctx, reindexProperties); err != nil {
		r.logError(err, "failed reindexing properties")
		return errors.Wrapf(err, "failed reindexing properties on shard '%s'", r.shard.name)
	}

	for i := range bucketsToReindex {
		if err := r.checkContextExpired(ctx, "replacing buckets stopped due to context canceled"); err != nil {
			return err
		}
		tempBucketName := helpers.TempBucketFromBucketName(bucketsToReindex[i])
		tempBucket := r.shard.store.Bucket(tempBucketName)
		tempBucket.FlushMemtable(ctx)
		tempBucket.UpdateStatus(storagestate.StatusReadOnly)

		if err := r.shard.store.ReplaceBuckets(ctx, bucketsToReindex[i], tempBucketName); err != nil {
			r.logError(err, "failed replacing buckets")
			return err
		}
		r.logger.
			WithField("action", "inverted reindex").
			WithField("shard", r.shard.name).
			WithField("bucket", bucketsToReindex[i]).
			WithField("temp_bucket", tempBucketName).
			Debug("replaced buckets")
	}

	if err := r.checkContextExpired(ctx, "resuming store stopped due to context canceled"); err != nil {
		return err
	}

	if err := r.resumeStoreActivity(ctx); err != nil {
		r.logError(err, "failed resuming store activity")
		return err
	}

	return nil
}

func (r *ShardInvertedReindexer) pauseStoreActivity(ctx context.Context) error {
	if err := r.shard.store.PauseCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed pausing compaction for shard '%s'", r.shard.name)
	}
	if err := r.shard.store.FlushMemtables(ctx); err != nil {
		return errors.Wrapf(err, "failed flushing memtables for shard '%s'", r.shard.name)
	}
	r.shard.store.UpdateBucketsStatus(storagestate.StatusReadOnly)

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.name).
		Debug("paused store activity")

	return nil
}

func (r *ShardInvertedReindexer) resumeStoreActivity(ctx context.Context) error {
	if err := r.shard.store.ResumeCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed resuming compaction for shard '%s'", r.shard.name)
	}
	r.shard.store.UpdateBucketsStatus(storagestate.StatusReady)

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.name).
		Debug("resumed store activity")

	return nil
}

func (r *ShardInvertedReindexer) createTempBucket(ctx context.Context, name string,
	strategy string, options ...lsmkv.BucketOption,
) error {
	tempName := helpers.TempBucketFromBucketName(name)
	bucketOptions := append(options, lsmkv.WithStrategy(strategy))

	if err := r.shard.store.CreateBucket(ctx, tempName, bucketOptions...); err != nil {
		return errors.Wrapf(err, "failed creating temp bucket '%s'", tempName)
	}

	// no point starting compaction until bucket successfully populated and plugged in
	if err := r.shard.store.Bucket(tempName).PauseCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed pausing compaction for temp bucket '%s'", tempName)
	}
	return nil
}

func (r *ShardInvertedReindexer) reindexProperties(ctx context.Context, reindexableProperties []ReindexableProperty) error {
	checker := newReindexablePropertyChecker(reindexableProperties)
	objectsBucket := r.shard.store.Bucket(helpers.ObjectsBucketLSM)

	r.logger.
		WithField("action", "inverted reindex").
		WithField("shard", r.shard.name).
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
				WithField("shard", r.shard.name).
				Debugf("iterating through objects: %d done", i)
		}
		docID := object.DocID()
		properties, nilProperties, err := r.shard.analyzeObject(object)
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
		WithField("shard", r.shard.name).
		Debugf("iterating through objects: %d done", i)

	return nil
}

func (r *ShardInvertedReindexer) handleProperty(ctx context.Context, checker *reindexablePropertyChecker,
	docID uint64, property inverted.Property,
) error {
	reindexableHashPropValue := checker.isReindexable(property.Name, IndexTypeHashPropValue)
	reindexablePropValue := checker.isReindexable(property.Name, IndexTypePropValue)

	if reindexableHashPropValue || reindexablePropValue {
		var hashBucketValue, bucketValue *lsmkv.Bucket

		if reindexableHashPropValue {
			hashBucketValue = r.tempBucket(property.Name, IndexTypeHashPropValue)
			if hashBucketValue == nil {
				return fmt.Errorf("no hash bucket for prop '%s' value found", property.Name)
			}
		}
		if reindexablePropValue {
			bucketValue = r.tempBucket(property.Name, IndexTypePropValue)
			if bucketValue == nil {
				return fmt.Errorf("no bucket for prop '%s' value found", property.Name)
			}
		}

		if property.HasFrequency {
			propLen := float32(len(property.Items))
			for _, item := range property.Items {
				key := item.Data
				if reindexableHashPropValue {
					if err := r.shard.addToPropertyHashBucket(hashBucketValue, key); err != nil {
						return errors.Wrapf(err, "failed adding to prop '%s' value hash bucket", property.Name)
					}
				}
				if reindexablePropValue {
					pair := r.shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
					if err := r.shard.addToPropertyMapBucket(bucketValue, pair, key); err != nil {
						return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
					}
				}
			}
		} else {
			for _, item := range property.Items {
				key := item.Data
				if reindexableHashPropValue {
					if err := r.shard.addToPropertyHashBucket(hashBucketValue, key); err != nil {
						return errors.Wrapf(err, "failed adding to prop '%s' value hash bucket", property.Name)
					}
				}
				if reindexablePropValue {
					if err := r.shard.addToPropertySetBucket(bucketValue, docID, key); err != nil {
						return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
					}
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
	if r.shard.index.invertedIndexConfig.IndexPropertyLength && property.Length >= 0 {
		key, err := r.shard.keyPropertyLength(property.Length)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' length", property.Name)
		}
		if checker.isReindexable(property.Name, IndexTypeHashPropLength) {
			hashBucketLength := r.tempBucket(property.Name, IndexTypeHashPropLength)
			if hashBucketLength == nil {
				return fmt.Errorf("no hash bucket for prop '%s' length found", property.Name)
			}
			if err := r.shard.addToPropertyHashBucket(hashBucketLength, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' length hash bucket", property.Name)
			}
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

	if r.shard.index.invertedIndexConfig.IndexNullState {
		key, err := r.shard.keyPropertyNull(property.Length == 0)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' null", property.Name)
		}
		if checker.isReindexable(property.Name, IndexTypeHashPropNull) {
			hashBucketNull := r.tempBucket(property.Name, IndexTypeHashPropNull)
			if hashBucketNull == nil {
				return fmt.Errorf("no hash bucket for prop '%s' null found", property.Name)
			}
			if err := r.shard.addToPropertyHashBucket(hashBucketNull, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' null hash bucket", property.Name)
			}
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
	if r.shard.index.invertedIndexConfig.IndexPropertyLength && nilProperty.AddToPropertyLength {
		key, err := r.shard.keyPropertyLength(0)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' length", nilProperty.Name)
		}
		if checker.isReindexable(nilProperty.Name, IndexTypeHashPropLength) {
			hashBucketLength := r.tempBucket(nilProperty.Name, IndexTypeHashPropLength)
			if hashBucketLength == nil {
				return fmt.Errorf("no hash bucket for prop '%s' length found", nilProperty.Name)
			}
			if err := r.shard.addToPropertyHashBucket(hashBucketLength, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' length hash bucket", nilProperty.Name)
			}
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

	if r.shard.index.invertedIndexConfig.IndexNullState {
		key, err := r.shard.keyPropertyNull(true)
		if err != nil {
			return errors.Wrapf(err, "failed creating key for prop '%s' null", nilProperty.Name)
		}
		if checker.isReindexable(nilProperty.Name, IndexTypeHashPropNull) {
			hashBucketNull := r.tempBucket(nilProperty.Name, IndexTypeHashPropNull)
			if hashBucketNull == nil {
				return fmt.Errorf("no hash bucket for prop '%s' null found", nilProperty.Name)
			}
			if err := r.shard.addToPropertyHashBucket(hashBucketNull, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' null hash bucket", nilProperty.Name)
			}
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
	CheckSupportedPropertyIndexType(indexType)

	switch indexType {
	case IndexTypePropValue:
		return helpers.BucketFromPropNameLSM(propName)
	case IndexTypePropLength:
		return helpers.BucketFromPropNameLengthLSM(propName)
	case IndexTypePropNull:
		return helpers.BucketFromPropNameNullLSM(propName)
	case IndexTypeHashPropValue:
		return helpers.HashBucketFromPropNameLSM(propName)
	case IndexTypeHashPropLength:
		return helpers.HashBucketFromPropNameLengthLSM(propName)
	case IndexTypeHashPropNull:
		return helpers.HashBucketFromPropNameNullLSM(propName)
	default:
		return ""
	}
}

func (r *ShardInvertedReindexer) tempBucket(propName string, indexType PropertyIndexType) *lsmkv.Bucket {
	tempBucketName := helpers.TempBucketFromBucketName(r.bucketName(propName, indexType))
	return r.shard.store.Bucket(tempBucketName)
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
		WithField("shard", r.shard.name).
		WithError(err).
		Errorf(msg, args...)
}
