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
	"strings"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type shardCycleCallbacks struct {
	compactionCallbacks        cyclemanager.CycleCallbackGroup
	compactionCallbacksCtrl    cyclemanager.CycleCallbackCtrl
	compactionAuxCallbacks     cyclemanager.CycleCallbackGroup
	compactionAuxCallbacksCtrl cyclemanager.CycleCallbackCtrl

	flushCallbacks     cyclemanager.CycleCallbackGroup
	flushCallbacksCtrl cyclemanager.CycleCallbackCtrl

	vectorCommitLoggerCallbacks     cyclemanager.CycleCallbackGroup
	vectorTombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup
	vectorCombinedCallbacksCtrl     cyclemanager.CycleCallbackCtrl

	geoPropsCommitLoggerCallbacks     cyclemanager.CycleCallbackGroup
	geoPropsTombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup
	geoPropsCombinedCallbacksCtrl     cyclemanager.CycleCallbackCtrl
}

func (s *Shard) initCycleCallbacks() {
	id := func(elems ...string) string {
		elems = append([]string{"shard", s.index.ID(), s.name}, elems...)
		return strings.Join(elems, "/")
	}

	// Multi-tenant collections back off per shard; single-tenant collections
	// back off at the collection (ticker) level instead. Only one of the two
	// should be active, otherwise the two backoffs interfere.
	withIntervals := func(intervals cyclemanager.CycleIntervals) cyclemanager.RegisterOption {
		if !s.index.partitioningEnabled {
			return nil
		}
		return cyclemanager.WithIntervals(intervals)
	}

	var compactionCallbacks cyclemanager.CycleCallbackGroup
	var compactionCallbacksCtrl cyclemanager.CycleCallbackCtrl
	var compactionAuxCallbacks cyclemanager.CycleCallbackGroup
	var compactionAuxCallbacksCtrl cyclemanager.CycleCallbackCtrl

	if s.index.cycleCallbacks.compactionAuxCallbacks == nil {
		compactionId := id("compaction")
		compactionCallbacks = cyclemanager.NewCallbackGroup(compactionId, s.index.logger, 1)
		compactionCallbacksCtrl = s.index.cycleCallbacks.compactionCallbacks.Register(
			compactionId, compactionCallbacks.CycleCallback,
			withIntervals(cyclemanager.CompactionCycleIntervals()))
		compactionAuxCallbacksCtrl = cyclemanager.NewCallbackCtrlNoop()
	} else {
		compactionId := id("compaction-non-objects")
		compactionCallbacks = cyclemanager.NewCallbackGroup(compactionId, s.index.logger, 1)
		compactionCallbacksCtrl = s.index.cycleCallbacks.compactionCallbacks.Register(
			compactionId, compactionCallbacks.CycleCallback,
			withIntervals(cyclemanager.CompactionCycleIntervals()))

		compactionAuxId := id("compaction-objects")
		compactionAuxCallbacks = cyclemanager.NewCallbackGroup(compactionAuxId, s.index.logger, 1)
		compactionAuxCallbacksCtrl = s.index.cycleCallbacks.compactionAuxCallbacks.Register(
			compactionAuxId, compactionAuxCallbacks.CycleCallback,
			withIntervals(cyclemanager.CompactionCycleIntervals()))
	}

	flushId := id("flush")
	flushCallbacks := cyclemanager.NewCallbackGroup(flushId, s.index.logger, 1)
	flushCallbacksCtrl := s.index.cycleCallbacks.flushCallbacks.Register(
		flushId, flushCallbacks.CycleCallback,
		withIntervals(cyclemanager.MemtableFlushCycleIntervals()))

	vectorCommitLoggerId := id("vector", "commit_logger")
	vectorCommitLoggerCallbacks := cyclemanager.NewCallbackGroup(vectorCommitLoggerId, s.index.logger, 1)
	vectorCommitLoggerCallbacksCtrl := s.index.cycleCallbacks.vectorCommitLoggerCallbacks.Register(
		vectorCommitLoggerId, vectorCommitLoggerCallbacks.CycleCallback,
		withIntervals(cyclemanager.HnswCommitLoggerCycleIntervals()))

	vectorTombstoneCleanupId := id("vector", "tombstone_cleanup")
	vectorTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup(vectorTombstoneCleanupId, s.index.logger, 1)
	// fixed interval on class level, no need to specify separate on shard level
	vectorTombstoneCleanupCallbacksCtrl := s.index.cycleCallbacks.vectorTombstoneCleanupCallbacks.Register(
		vectorTombstoneCleanupId, vectorTombstoneCleanupCallbacks.CycleCallback)

	vectorCombinedCallbacksCtrl := cyclemanager.NewCombinedCallbackCtrl(2, s.index.logger,
		vectorCommitLoggerCallbacksCtrl, vectorTombstoneCleanupCallbacksCtrl)

	geoPropsCommitLoggerId := id("geo_props", "commit_logger")
	geoPropsCommitLoggerCallbacks := cyclemanager.NewCallbackGroup(geoPropsCommitLoggerId, s.index.logger, 1)
	geoPropsCommitLoggerCallbacksCtrl := s.index.cycleCallbacks.geoPropsCommitLoggerCallbacks.Register(
		geoPropsCommitLoggerId, geoPropsCommitLoggerCallbacks.CycleCallback,
		withIntervals(cyclemanager.GeoCommitLoggerCycleIntervals()))

	geoPropsTombstoneCleanupId := id("geoProps", "tombstone_cleanup")
	geoPropsTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup(geoPropsTombstoneCleanupId, s.index.logger, 1)
	// fixed interval on class level, no need to specify separate on shard level
	geoPropsTombstoneCleanupCallbacksCtrl := s.index.cycleCallbacks.geoPropsTombstoneCleanupCallbacks.Register(
		geoPropsTombstoneCleanupId, geoPropsTombstoneCleanupCallbacks.CycleCallback)

	geoPropsCombinedCallbacksCtrl := cyclemanager.NewCombinedCallbackCtrl(2, s.index.logger,
		geoPropsCommitLoggerCallbacksCtrl, geoPropsTombstoneCleanupCallbacksCtrl)

	s.cycleCallbacks = &shardCycleCallbacks{
		compactionCallbacks:        compactionCallbacks,
		compactionCallbacksCtrl:    compactionCallbacksCtrl,
		compactionAuxCallbacks:     compactionAuxCallbacks,
		compactionAuxCallbacksCtrl: compactionAuxCallbacksCtrl,

		flushCallbacks:     flushCallbacks,
		flushCallbacksCtrl: flushCallbacksCtrl,

		vectorCommitLoggerCallbacks:     vectorCommitLoggerCallbacks,
		vectorTombstoneCleanupCallbacks: vectorTombstoneCleanupCallbacks,
		vectorCombinedCallbacksCtrl:     vectorCombinedCallbacksCtrl,

		geoPropsCommitLoggerCallbacks:     geoPropsCommitLoggerCallbacks,
		geoPropsTombstoneCleanupCallbacks: geoPropsTombstoneCleanupCallbacks,
		geoPropsCombinedCallbacksCtrl:     geoPropsCombinedCallbacksCtrl,
	}
}
