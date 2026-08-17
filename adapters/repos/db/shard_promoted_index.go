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

	"github.com/weaviate/weaviate/entities/models"
)

// armPromotedIndex makes ordinary writes reach propName's indexType bucket
// while the schema still advertises that index as disabled. See the
// [Shard.promotedIndexes] field godoc for why the window exists.
//
// Arming a property means the analyzer stops dropping it, which brings back
// two arms of the write path the migration itself never needed: the
// null-state one, which has no data-type gate at all, and the property-length
// one, which is gated on a value rather than a type. So the buckets those
// arms write to are created here, before the first armed write, exactly as
// shard init would create them for a property whose flag is already true.
//
// A promoted index whose bucket is not open on this shard is not armed: that
// happens on the recovery path that renames directories without loading them,
// where an armed write would find no bucket and fail. Such a shard picks the
// arming up at its next load, from the record.
func (s *Shard) armPromotedIndex(ctx context.Context, propName, indexType string) error {
	if propName == "" || indexType == "" || s.isPromotedIndexArmed(propName, indexType) {
		return nil
	}
	mainBucket, ok := mainBucketForPropertyIndex(propName, indexType)
	if !ok {
		return fmt.Errorf("no main bucket for index type %q", indexType)
	}
	if s.store.Bucket(mainBucket) == nil {
		s.index.logger.WithField("shard", s.name).WithField("property", propName).
			Debugf("promoted %s index is not open on this shard; leaving writes to the schema until the next load arms it from the record", indexType)
		return nil
	}
	if err := s.createPromotedIndexSidecarBuckets(ctx, propName); err != nil {
		return err
	}

	s.promotedIndexesMu.Lock()
	defer s.promotedIndexesMu.Unlock()
	if s.promotedIndexes == nil {
		s.promotedIndexes = map[string]map[string]struct{}{}
	}
	if s.promotedIndexes[propName] == nil {
		s.promotedIndexes[propName] = map[string]struct{}{}
	}
	s.promotedIndexes[propName][indexType] = struct{}{}
	return nil
}

// armPromotedIndexes arms every property of one completed migration, logging
// rather than returning failures: the swap that calls this has already
// committed its data work, and a shard that comes up unarmed loses window
// writes rather than the index itself.
func (s *Shard) armPromotedIndexes(ctx context.Context, propNames []string, indexType string) {
	for _, propName := range propNames {
		if err := s.armPromotedIndex(ctx, propName, indexType); err != nil {
			s.index.logger.WithField("shard", s.name).WithField("property", propName).
				Errorf("failed to route writes into the promoted %s index; writes until the schema flip will be missing from it: %v",
					indexType, err)
		}
	}
}

// armFinalizedMigrations re-establishes, from the records on disk, what the
// swap that wrote them established in memory: writes reach an index the
// schema does not advertise yet, and are analyzed under the tokenization the
// property's keys are actually stored under.
//
// The tokenization half re-arms the same per-shard overlay a live migration
// sets, so the query path picks it up too — reads and writes on a
// retokenized property have to agree on which tokenization the bucket holds,
// and after a restart neither of them would otherwise know.
func (s *Shard) armFinalizedMigrations(ctx context.Context, finalized finalizedMigrations) {
	if finalized.empty() {
		return
	}
	for propName, indexTypes := range finalized.indexes {
		for indexType := range indexTypes {
			s.armPromotedIndexes(ctx, []string{propName}, indexType)
		}
	}
	for propName, target := range finalized.tokenizations {
		s.SetTokenizationOverlay(propName, target)
	}
}

// disarmPromotedIndex stops routing writes into a promoted index, because the
// schema has caught up (the write path resolves the index from the schema
// again) or because the index is gone (there is nothing left to write to).
func (s *Shard) disarmPromotedIndex(propName, indexType string) {
	s.promotedIndexesMu.Lock()
	defer s.promotedIndexesMu.Unlock()
	indexTypes, ok := s.promotedIndexes[propName]
	if !ok {
		return
	}
	delete(indexTypes, indexType)
	if len(indexTypes) == 0 {
		delete(s.promotedIndexes, propName)
	}
}

func (s *Shard) isPromotedIndexArmed(propName, indexType string) bool {
	s.promotedIndexesMu.RLock()
	defer s.promotedIndexesMu.RUnlock()
	_, ok := s.promotedIndexes[propName][indexType]
	return ok
}

// promotedIndexesFor returns the armed index types of the named properties.
// Nil when none of them is armed, which is the steady state.
func (s *Shard) promotedIndexesFor(propNames []string) map[string][]string {
	s.promotedIndexesMu.RLock()
	defer s.promotedIndexesMu.RUnlock()
	if len(s.promotedIndexes) == 0 {
		return nil
	}
	var out map[string][]string
	for _, propName := range propNames {
		indexTypes := s.promotedIndexes[propName]
		if len(indexTypes) == 0 {
			continue
		}
		if out == nil {
			out = make(map[string][]string, len(propNames))
		}
		for indexType := range indexTypes {
			out[propName] = append(out[propName], indexType)
		}
	}
	return out
}

// createPromotedIndexSidecarBuckets creates the null-state and
// property-length buckets of a property whose value index a migration
// promoted. Both are no-ops when the shard does not index that state at all,
// and when the property's data type has none.
func (s *Shard) createPromotedIndexSidecarBuckets(ctx context.Context, propName string) error {
	prop := s.propertyForPromotedIndex(propName)
	if prop == nil || len(prop.DataType) == 0 {
		return fmt.Errorf("property %q is not in the schema this shard can see", propName)
	}
	if s.index.invertedIndexConfig.IndexNullState {
		if err := s.createPropertyNullIndex(ctx, prop, s.makeDefaultBucketOptions); err != nil {
			return fmt.Errorf("null index of %q: %w", propName, err)
		}
	}
	if s.index.invertedIndexConfig.IndexPropertyLength {
		if err := s.createPropertyLengthIndex(ctx, prop, s.makeDefaultBucketOptions); err != nil {
			return fmt.Errorf("length index of %q: %w", propName, err)
		}
	}
	return nil
}

// propertyForPromotedIndex resolves a property by name. The live schema
// answers first: a property added after this shard loaded is missing from the
// class the shard was built with.
func (s *Shard) propertyForPromotedIndex(propName string) *models.Property {
	if s.class != nil {
		if live := s.index.getSchema.ReadOnlyClass(s.class.Class); live != nil {
			if prop := propertyNamed(live.Properties, propName); prop != nil {
				return prop
			}
		}
		if prop := propertyNamed(s.class.Properties, propName); prop != nil {
			return prop
		}
	}
	return nil
}

func propertyNamed(props []*models.Property, propName string) *models.Property {
	for _, prop := range props {
		if prop != nil && prop.Name == propName {
			return prop
		}
	}
	return nil
}
