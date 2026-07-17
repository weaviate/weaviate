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
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) extendInvertedIndicesLSM(props []inverted.Property, nilProps []inverted.NilProperty,
	docID uint64,
) error {
	for _, prop := range props {
		if err := s.addToPropertyValueIndex(docID, prop); err != nil {
			return err
		}

		// add non-nil properties to the null-state inverted index, but skip internal properties (__meta_count, _id etc)
		if isMetaCountProperty(prop) || isInternalProperty(prop) {
			continue
		}

		// properties where defining a length does not make sense (floats etc.) have a negative entry as length
		if err := s.writeLengthNullSidecar(prop.Name, docID, prop.Length >= 0, prop.Length, prop.Length == 0,
			"add indexed property length", "add indexed null state"); err != nil {
			return err
		}
	}

	// add nil properties to the nullstate and property length inverted index
	for _, nilProperty := range nilProps {
		if err := s.writeLengthNullSidecar(nilProperty.Name, docID, nilProperty.AddToPropertyLength, 0, true,
			"add indexed property length", "add indexed null state"); err != nil {
			return err
		}
	}

	return nil
}

// writeLengthNullSidecar writes the property-length and null-state sidecar
// entries for a single (non-nil or nil) property to the live length/null
// LSM buckets. Shared by the normal write path (extendInvertedIndicesLSM,
// unscoped) and the migration backfill path (backfillSidecarsForMigration,
// scoped to propsInScope).
//
// `writeLength` gates the length write independently of `length` itself:
// the non-nil-property caller gates on `prop.Length >= 0` (properties where
// a length doesn't make sense, e.g. floats, use a negative sentinel), while
// the nil-property caller gates on `nilProperty.AddToPropertyLength` and
// always passes length 0. `lengthErrMsg`/`nullErrMsg` let each caller keep
// its own error-wrap text.
func (s *Shard) writeLengthNullSidecar(name string, docID uint64, writeLength bool, length int, isNull bool,
	lengthErrMsg, nullErrMsg string,
) error {
	if s.index.invertedIndexConfig.IndexPropertyLength && writeLength {
		if err := s.addToPropertyLengthIndex(name, docID, length); err != nil {
			return errors.Wrap(err, lengthErrMsg)
		}
	}

	if s.index.invertedIndexConfig.IndexNullState {
		if err := s.addToPropertyNullIndex(name, docID, isNull); err != nil {
			return errors.Wrap(err, nullErrMsg)
		}
	}

	return nil
}

// backfillSidecarsForMigration writes the null-state / property-length
// sidecar entries that an enable-* runtime migration's backfill loop would
// otherwise skip for pre-existing objects (weaviate/0-weaviate-issues#322).
// Mirrors extendInvertedIndicesLSM's gating logic, restricted to
// propsInScope - the set of properties THIS migration targets.
//
// Scoping to propsInScope is load-bearing, not cosmetic: props/nilProps
// cover every analyzed property on the object, including ones already
// correctly backfilled before this migration started. Re-writing
// null/length for those is harmless (RoaringSet Add is idempotent), but
// re-tallying BM25 stats for them is not - see
// TestSidecarBackfill_ScopedToMigratingPropOnly.
//
// Null/length buckets are the same live buckets the normal write path
// uses, so writing into them here needs no swap/versioning changes and is
// safe under the existing pause/resume tick boundary.
//
// The BM25 prop-length tally is deliberately NOT updated here. Do not add
// a per-object TrackProperty call to this loop:
//  1. This function runs for every generic migration (map->blockmax,
//     RebuildSearchable, retokenize, ...), not just enable-*.
//     prop.HasSearchableIndex is true for any property already searchable
//     before this migration started, so a per-object tally write would
//     double Sum/Count for those.
//  2. A per-tick cumulative counter increment is not idempotent like the
//     RoaringSet writes above - a crash/resume replaying a tick would
//     double-count, and without an explicit flush a crash right after
//     migration completion would lose the tally entirely.
//
// The tally is instead recomputed once, from scratch, per migrating
// property whose overlay forces IndexSearchable on, after the migration
// has fully swapped - see ShardReindexTaskGeneric.completeMigrationOnShard
// and Shard.recomputeSearchableTallyForProp.
func (s *Shard) backfillSidecarsForMigration(docID uint64, props []inverted.Property,
	nilProps []inverted.NilProperty, propsInScope map[string]struct{},
) error {
	for _, prop := range props {
		if _, ok := propsInScope[prop.Name]; !ok {
			continue
		}
		if isMetaCountProperty(prop) || isInternalProperty(prop) {
			continue
		}

		// properties where defining a length does not make sense (floats
		// etc.) have a negative entry as length - mirrors extendInvertedIndicesLSM.
		if err := s.writeLengthNullSidecar(prop.Name, docID, prop.Length >= 0, prop.Length, prop.Length == 0,
			"backfill: add indexed property length", "backfill: add indexed null state"); err != nil {
			return err
		}
	}

	for _, nilProperty := range nilProps {
		if _, ok := propsInScope[nilProperty.Name]; !ok {
			continue
		}

		// Nil properties are never fed to the BM25 tally on the normal
		// write path either (SetPropertyLengths only iterates `props`,
		// never `nilprops` - see shard_write_put.go:562) - no tally call
		// here, matching that behavior exactly.
		if err := s.writeLengthNullSidecar(nilProperty.Name, docID, nilProperty.AddToPropertyLength, 0, true,
			"backfill: add indexed property length (nil)", "backfill: add indexed null state (nil)"); err != nil {
			return err
		}
	}

	return nil
}

// recomputeSearchableTallyForProp rebuilds the BM25 prop-length tally for a
// single property from scratch by scanning every object on the shard,
// following the same clear-then-rescan pattern as
// Migrator.RecountProperties, scoped to one property on one shard.
//
// Called once, post-swap, by ShardReindexTaskGeneric.completeMigrationOnShard
// for a property whose migration overlay forces IndexSearchable on
// (weaviate/0-weaviate-issues#322). overlay must be the same
// AnalyzerOverlay the migration used, so objects written before the live
// schema flag flips still analyze with HasSearchableIndex=true for this
// property.
//
// Idempotent by construction: ResetProperty followed by a full rescan
// produces the same end state from any crash point. Flushes the tracker to
// disk before returning.
//
// Flushes the objects bucket's memtable before opening the rescan cursor -
// CursorOnDisk only sees segment-resident data, so an unflushed write
// would otherwise be invisible to the rescan.
//
// Three windows matter for the migrating prop's tally, only one a real
// residual:
//
//  1. A write reaching this task's double-write callbacks (registered
//     before backfill, disabled after runtimeSwap) is tallied
//     incrementally by EnableSearchableStrategy's callbacks - see
//     trackMigratingPropLength. A property with no other live index never
//     reaches them for a brand-new value; window 2 covers that write
//     instead.
//  2. This function's flush-then-rescan covers every write that landed in
//     the objects bucket before the flush, regardless of window 1: the
//     rescan re-derives the value from raw stored JSON, and ResetProperty
//     first discards any window-1 increment, so the rebuild is
//     exactly-once.
//  3. The sole residual is a write racing strictly between the flush and
//     the cursor open: it lands in the new active memtable, not lost,
//     only delayed - any future invocation (idempotent) converges the
//     tally, e.g. a recovery re-entry into completeMigrationOnShard.
func (s *Shard) recomputeSearchableTallyForProp(ctx context.Context, propName string,
	overlay map[string]inverted.PropertyOverlay,
) error {
	tracker := s.GetPropertyLengthTracker()
	tracker.ResetProperty(propName)

	objectsBucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket == nil {
		return errors.New("recompute searchable tally: objects bucket not found")
	}
	if err := objectsBucket.FlushAndSwitch(); err != nil {
		return errors.Wrap(err, "recompute searchable tally: flush objects bucket before rescan")
	}
	cursor := objectsBucket.CursorOnDisk()
	defer cursor.Close()

	className := s.index.Config.ClassName.String()
	addProps := additional.Properties{}
	propExtraction := storobj.NewPropExtraction().Add(propName)

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "recompute searchable tally")
		}

		obj, err := storobj.FromBinaryOptionalDisk(v, className, addProps, propExtraction)
		if err != nil {
			return errors.Wrap(err, "recompute searchable tally: unmarshal object")
		}

		props, _, err := s.AnalyzeObjectForMigrationWithOverlay(obj, overlay)
		if err != nil {
			return errors.Wrap(err, "recompute searchable tally: analyze object")
		}

		for _, prop := range props {
			if prop.Name != propName || !prop.HasSearchableIndex {
				continue
			}
			if err := tracker.TrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
				return errors.Wrap(err, "recompute searchable tally: track property")
			}
		}
	}

	if err := tracker.Flush(); err != nil {
		return errors.Wrap(err, "recompute searchable tally: flush")
	}

	return nil
}

func (s *Shard) addToPropertyValueIndex(docID uint64, property inverted.Property) error {
	if property.HasFilterableIndex {
		bucketValue := s.store.Bucket(helpers.BucketFromPropNameLSM(property.Name))
		if bucketValue == nil {
			return errors.Errorf("no bucket for prop '%s' found", property.Name)
		}

		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertySetBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if property.HasSearchableIndex {
		bucketValue := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM(property.Name))
		if bucketValue == nil {
			return errors.Errorf("no bucket searchable for prop '%s' found", property.Name)
		}
		propLen := float32(0)

		if bucketValue.Strategy() == lsmkv.StrategyInverted {
			// Iterating over all items to calculate the property length, which is the sum of all term frequencies
			for _, item := range property.Items {
				propLen += item.TermFrequency
			}
		} else {
			// This is the old way of calculating the property length, which counts terms that show up multiple times only once,
			// which is not standard for BM25
			propLen = float32(len(property.Items))
		}
		for _, item := range property.Items {
			key := item.Data
			pair := s.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := s.addToPropertyMapBucket(bucketValue, pair, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if property.HasRangeableIndex {
		bucketValue := s.store.Bucket(helpers.BucketRangeableFromPropNameLSM(property.Name))
		if bucketValue == nil {
			return errors.Errorf("no bucket rangeable for prop '%s' found", property.Name)
		}

		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertyRangeBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if err := s.onAddToPropertyValueIndex(docID, &property); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addToPropertyLengthIndex(propName string, docID uint64, length int) error {
	bucketLength := s.store.Bucket(helpers.BucketFromPropNameLengthLSM(propName))
	if bucketLength == nil {
		return errors.Errorf("no bucket for prop '%s' length found", propName)
	}

	key, err := bucketKeyPropertyLength(length)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' length", propName)
	}
	if err := s.addToPropertySetBucket(bucketLength, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' length bucket", propName)
	}
	return nil
}

func (s *Shard) addToPropertyNullIndex(propName string, docID uint64, isNull bool) error {
	bucketNull := s.store.Bucket(helpers.BucketFromPropNameNullLSM(propName))
	if bucketNull == nil {
		return errors.Errorf("no bucket for prop '%s' null found", propName)
	}

	key, err := bucketKeyPropertyNull(isNull)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' null", propName)
	}
	if err := s.addToPropertySetBucket(bucketNull, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' null bucket", propName)
	}
	return nil
}

func (s *Shard) pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	// 8 bytes for doc id, 4 bytes for frequency, 4 bytes for prop term length
	buf := make([]byte, 16)

	// Shard Index version 2 requires BigEndian for sorting, if the shard was
	// built prior assume it uses LittleEndian
	if s.versioner.Version() < 2 {
		binary.LittleEndian.PutUint64(buf[0:8], docID)
	} else {
		binary.BigEndian.PutUint64(buf[0:8], docID)
	}
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(freq))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(propLen))

	return lsmkv.MapPair{
		Key:   buf[:8],
		Value: buf[8:],
	}
}

func (s *Shard) addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyInverted)

	return bucket.MapSet(key, pair)
}

func (s *Shard) addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet)

	if bucket.Strategy() == lsmkv.StrategySetCollection {
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)

		return bucket.SetAdd(key, [][]byte{docIDBytes})
	}

	return bucket.RoaringSetAddOne(key, docID)
}

func (s *Shard) addToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyRoaringSetRange)

	if len(key) != 8 {
		return fmt.Errorf("shard: invalid value length %d, should be 8 bytes", len(key))
	}

	return bucket.RoaringSetRangeAdd(binary.BigEndian.Uint64(key), docID)
}

func (s *Shard) batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket,
	item inverted.MergeItem,
) error {
	if b.Strategy() != lsmkv.StrategySetCollection && b.Strategy() != lsmkv.StrategyRoaringSet {
		panic("prop has no frequency, but bucket does not have 'Set' nor 'RoaringSet' strategy")
	}

	if b.Strategy() == lsmkv.StrategyRoaringSet {
		docIDs := make([]uint64, len(item.DocIDs))
		for i, idTuple := range item.DocIDs {
			docIDs[i] = idTuple.DocID
		}
		return b.RoaringSetAddList(item.Data, docIDs)
	}

	docIDs := make([][]byte, len(item.DocIDs))
	for i, idTuple := range item.DocIDs {
		docIDs[i] = make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDs[i], idTuple.DocID)
	}

	return b.SetAdd(item.Data, docIDs)
}

func (s *Shard) SetPropertyLengths(props []inverted.Property) error {
	for _, prop := range props {
		if !prop.HasSearchableIndex {
			continue
		}

		if err := s.GetPropertyLengthTracker().TrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
			return err
		}

	}

	return nil
}

func (s *Shard) subtractPropLengths(props []inverted.Property) error {
	for _, prop := range props {
		if !prop.HasSearchableIndex {
			continue
		}

		if err := s.GetPropertyLengthTracker().UnTrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
			return err
		}

	}

	return nil
}

var uniqueCounter atomic.Uint64

// GenerateUniqueString generates a random string of the specified length
func GenerateUniqueString(length int) (string, error) {
	uniqueCounter.Add(1)
	return fmt.Sprintf("%v", uniqueCounter.Load()), nil
}

// Empty the dimensions bucket, quickly and efficiently
func (s *Shard) resetDimensionsLSM(ctx context.Context) error {
	// Load the current one, or an empty one if it doesn't exist
	err := s.createDimensionsBucket(context.Background(), helpers.DimensionsBucketLSM)
	if err != nil {
		return fmt.Errorf("create dimensions bucket: %w", err)
	}

	// Fetch the actual bucket
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("resetDimensionsLSM: no bucket dimensions")
	}

	// Create random bucket name
	name, err := GenerateUniqueString(32)
	if err != nil {
		return errors.Wrap(err, "generate unique bucket name")
	}

	// Create a new bucket with the unique name
	err = s.createDimensionsBucket(context.Background(), name)
	if err != nil {
		return errors.Wrap(err, "create temporary dimensions bucket")
	}

	// Replace the old bucket with the new one
	err = s.store.ReplaceBuckets(context.Background(), helpers.DimensionsBucketLSM, name)
	if err != nil {
		return errors.Wrap(err, "replace dimensions bucket")
	}

	return nil
}

func (s *Shard) onAddToPropertyValueIndex(docID uint64, property *inverted.Property) error {
	callbacks, _ := s.callbacksAddToPropertyValueIndex.Load().([]onAddToPropertyValueIndex)
	ec := errorcompounder.New()
	for _, cb := range callbacks {
		ec.Add(cb(s, docID, property))
	}
	return ec.ToError()
}

func isMetaCountProperty(property inverted.Property) bool {
	return len(property.Name) > len(schema.InternalMetaCountSuffix) &&
		strings.HasSuffix(property.Name, schema.InternalMetaCountSuffix)
}

func isInternalProperty(property inverted.Property) bool {
	return property.Name[0] == '_'
}
