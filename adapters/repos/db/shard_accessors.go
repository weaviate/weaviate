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

// Some standard accessors for the shard struct.
// It is important to NEVER access the shard struct directly, because we lazy load shards, so the information might not be there.
package db

import (
	"maps"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/schema"
)

// ForEachVectorIndex iterates through each vector index initialized in the shard (named and legacy).
// Iteration stops at the first return of non-nil error. The callback runs under the read lock.
func (s *Shard) ForEachVectorIndex(f func(targetVector string, index VectorIndex) error) error {
	return s.vectors.ForEach(func(targetVector string, index VectorIndex, _ *VectorIndexQueue) error {
		return f(targetVector, index)
	})
}

// ForEachVectorQueue iterates through each vector index queue initialized in the shard (named and legacy).
// Iteration stops at the first return of non-nil error. The callback runs under the read lock.
func (s *Shard) ForEachVectorQueue(f func(targetVector string, queue *VectorIndexQueue) error) error {
	return s.vectors.ForEach(func(targetVector string, _ VectorIndex, queue *VectorIndexQueue) error {
		return f(targetVector, queue)
	})
}

// GetVectorIndexQueue retrieves a vector index queue associated with the targetVector.
// Empty targetVector is treated as a request to access a queue for the legacy vector index.
func (s *Shard) GetVectorIndexQueue(targetVector string) (*VectorIndexQueue, bool) {
	slot, ok := s.vectors.get(targetVector)
	if !ok {
		return nil, false
	}
	return slot.queue, true
}

// GetVectorIndex retrieves a vector index associated with the targetVector.
// Empty targetVector is treated as a request to access the legacy vector index.
func (s *Shard) GetVectorIndex(targetVector string) (VectorIndex, bool) {
	slot, ok := s.vectors.get(targetVector)
	if !ok {
		return nil, false
	}
	return slot.index, true
}

// WithVectorIndex runs f on the targetVector's index under a lease: a drop
// of that vector waits until f returns. found is false, and f is not
// called, when the shard has no such index. Empty targetVector is the
// legacy vector index.
func (s *Shard) WithVectorIndex(targetVector string, f func(index VectorIndex) error) (found bool, err error) {
	slot, release, ok := s.vectors.Acquire(targetVector)
	if !ok {
		return false, nil
	}
	defer release()
	return true, f(slot.index)
}

// WithVectorIndexQueue is WithVectorIndex for the vector's queue.
func (s *Shard) WithVectorIndexQueue(targetVector string, f func(queue *VectorIndexQueue) error) (found bool, err error) {
	slot, release, ok := s.vectors.Acquire(targetVector)
	if !ok {
		return false, nil
	}
	defer release()
	return true, f(slot.queue)
}

// AcquireVectorIndex hands out the targetVector's index under a lease the
// caller owns: it must call release when done, on every path, or a drop of
// that vector waits for the drain timeout. For a hold that spans a loop;
// a single call uses WithVectorIndex.
func (s *Shard) AcquireVectorIndex(targetVector string) (index VectorIndex, release func(), ok bool) {
	slot, release, ok := s.vectors.Acquire(targetVector)
	if !ok {
		return nil, nil, false
	}
	return slot.index, release, true
}

// AcquireVectorIndexQueue is AcquireVectorIndex for the vector's queue.
func (s *Shard) AcquireVectorIndexQueue(targetVector string) (queue *VectorIndexQueue, release func(), ok bool) {
	slot, release, ok := s.vectors.Acquire(targetVector)
	if !ok {
		return nil, nil, false
	}
	return slot.queue, release, true
}

func (s *Shard) hasLegacyVectorIndex() bool {
	_, ok := s.vectors.get("")
	return ok
}

func (s *Shard) hasAnyVectorIndex() bool {
	return s.vectors.Len() > 0
}

func (s *Shard) Versioner() *shardVersioner {
	return s.versioner
}

func (s *Shard) Index() *Index {
	return s.index
}

// Shard name(identifier?)
func (s *Shard) Name() string {
	return s.name
}

// The physical data store
func (s *Shard) Store() *lsmkv.Store {
	return s.store
}

func (s *Shard) Counter() *indexcounter.Counter {
	return s.counter
}

// Tracks the lengths of all properties.  Must be updated on inserts/deletes.
func (s *Shard) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	return s.propLenTracker
}

// Tracks the lengths of all properties.  Must be updated on inserts/deletes.
func (s *Shard) SetPropertyLengthTracker(tracker *inverted.JsonShardMetaData) {
	s.propLenTracker = tracker
}

// Grafana metrics
func (s *Shard) Metrics() *Metrics {
	return s.metrics
}

func (s *Shard) setFallbackToSearchable(fallback bool) {
	s.fallbackToSearchable = fallback
}

func (s *Shard) addJobToQueue(job job) {
	s.centralJobQueue <- job
}

// ForEachGeoQueue iterates through each geo index queue initialized in the shard.
// Iteration stops at the first return of non-nil error.
func (s *Shard) ForEachGeoQueue(f func(propName string, queue *VectorIndexQueue) error) error {
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	for propName, q := range s.geoQueues {
		if q == nil {
			continue
		}

		if err := f(propName, q); err != nil {
			return err
		}
	}
	return nil
}

// ForEachGeoIndex iterates through each geo index initialized in the shard.
// Iteration stops at the first return of non-nil error.
func (s *Shard) ForEachGeoIndex(f func(propName string, index *geo.Index) error) error {
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	for propName, idx := range s.propertyIndices {
		if idx.Type != schema.DataTypeGeoCoordinates || idx.GeoIndex == nil {
			continue
		}

		if err := f(propName, idx.GeoIndex); err != nil {
			return err
		}
	}
	return nil
}

// propertyIndicesSnapshot copies the property-specific indices for a searcher
// to read after this returns. Handing out the live map instead would race with
// initGeoProp and DropAll, which is fatal rather than recoverable. The copy is
// shallow: the *geo.Index values stay shared, so a concurrent drop reaches them.
func (s *Shard) propertyIndicesSnapshot() propertyspecific.Indices {
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	if len(s.propertyIndices) == 0 {
		return nil
	}
	return maps.Clone(s.propertyIndices)
}

func (s *Shard) hasGeoIndex() bool {
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	for _, idx := range s.propertyIndices {
		if idx.Type == schema.DataTypeGeoCoordinates {
			return true
		}
	}
	return false
}

func (s *Shard) hasGeoIndexForProp(propName string) bool {
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	return s.propertyIndices[propName].Type == schema.DataTypeGeoCoordinates
}
