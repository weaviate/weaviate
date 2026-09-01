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
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
)

// ForEachVectorIndex iterates through each vector index initialized in the shard (named and legacy).
// Iteration stops at the first return of non-nil error.
func (s *Shard) ForEachVectorIndex(f func(targetVector string, index VectorIndex) error) error {
	// As we expect the mutex to be write-locked very rarely, we allow the callback
	// to be invoked under the lock. If we find contention here, we should make a copy of the indexes
	// before iterating over them.
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	for targetVector, idx := range s.vectorIndexes {
		if idx == nil {
			continue
		}

		if err := f(targetVector, idx); err != nil {
			return err
		}
	}
	if s.vectorIndex != nil {
		if err := f("", s.vectorIndex); err != nil {
			return err
		}
	}
	return nil
}

// ForEachVectorQueue iterates through each vector index queue initialized in the shard (named and legacy).
// Iteration stops at the first return of non-nil error.
func (s *Shard) ForEachVectorQueue(f func(targetVector string, queue *VectorIndexQueue) error) error {
	// As we expect the mutex to be write-locked very rarely, we allow the callback
	// to be invoked under the lock. If we find contention here, we should make a copy of the queues
	// before iterating over them.
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	for targetVector, q := range s.queues {
		if q == nil {
			continue
		}

		if err := f(targetVector, q); err != nil {
			return err
		}
	}
	if s.queue != nil {
		if err := f("", s.queue); err != nil {
			return err
		}
	}
	return nil
}

// GetVectorIndexQueue retrieves a vector index queue associated with the targetVector.
// Empty targetVector is treated as a request to access a queue for the legacy vector index.
func (s *Shard) GetVectorIndexQueue(targetVector string) (*VectorIndexQueue, bool) {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	if s.isTargetVectorLegacyWithLock(targetVector) {
		return s.queue, s.queue != nil
	}

	queue, ok := s.queues[targetVector]
	return queue, ok
}

// GetVectorIndex retrieves a vector index queue associated with the targetVector.
// Empty targetVector is treated as a request to access a queue for the legacy vector index.
func (s *Shard) GetVectorIndex(targetVector string) (VectorIndex, bool) {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	if s.isTargetVectorLegacyWithLock(targetVector) {
		return s.vectorIndex, s.vectorIndex != nil
	}

	index, ok := s.vectorIndexes[targetVector]
	return index, ok
}

const msgVectorRefReleasedMoreThanOnce = "vector index reference released more than once per pin"

// pinVectorIndex resolves targetVector like [Shard.GetVectorIndex] and, in the
// same critical section, pins it for the caller's whole operation:
// [Shard.DropVectorIndex] waits for every pin to be released before it removes
// the index and the buckets it reads from. Resolving without pinning is what
// let a drop pull those buckets out from under a running search.
//
// Taking the reference under the same RLock as the map read is what makes the
// pin airtight: a drop claims its target under vectorIndexMu.Lock, so a pin
// either completes before the claim and is seen by the drain, or observes the
// claim and is refused.
//
// release is never nil, including when the index is not found, and must be
// called exactly once — defer it at the call site.
func (s *Shard) pinVectorIndex(targetVector string) (VectorIndex, func(), bool) {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	var (
		key   string
		index VectorIndex
		ok    bool
	)
	if s.isTargetVectorLegacyWithLock(targetVector) {
		key, index, ok = "", s.vectorIndex, s.vectorIndex != nil
	} else {
		key = targetVector
		index, ok = s.vectorIndexes[targetVector]
	}
	if !ok {
		return nil, func() {}, false
	}
	if s.vectorIndexDropping[key] > 0 {
		return nil, func() {}, false
	}

	refs := s.vectorIndexRefsFor(key)
	refs.Add(1)

	// Releasing more than once per pin would drive the counter negative and
	// disable the drain, so absorb it and report it.
	var released atomic.Bool
	return index, func() {
		if !released.CompareAndSwap(false, true) {
			s.index.logger.
				WithField("action", "vector_index_ref_count").
				WithField("shard", s.name).
				WithField("target_vector", key).
				Error(msgVectorRefReleasedMoreThanOnce)
			return
		}
		refs.Add(-1)
	}, true
}

// vectorIndexRefsFor returns the reference counter of one target vector,
// creating it on first use. Callers hold at least vectorIndexMu.RLock, so the
// counters live in a sync.Map rather than a plain map guarded by it.
func (s *Shard) vectorIndexRefsFor(key string) *atomic.Int64 {
	if refs, ok := s.vectorIndexRefs.Load(key); ok {
		return refs.(*atomic.Int64)
	}
	refs, _ := s.vectorIndexRefs.LoadOrStore(key, &atomic.Int64{})
	return refs.(*atomic.Int64)
}

func (s *Shard) isTargetVectorLegacyWithLock(targetVector string) bool {
	if targetVector == "" {
		return true
	}

	return s.vectorIndex != nil && targetVector == modelsext.DefaultNamedVectorName
}

func (s *Shard) hasLegacyVectorIndex() bool {
	_, ok := s.GetVectorIndex("")
	return ok
}

func (s *Shard) hasAnyVectorIndex() bool {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	return len(s.vectorIndexes) > 0 || s.vectorIndex != nil
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
