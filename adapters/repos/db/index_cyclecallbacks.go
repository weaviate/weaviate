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
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type indexCycleCallbacks struct {
	// collects collections of compaction callbacks of stores (shards)
	compactionCallbacks cyclemanager.CycleCallbacks
	compactionCycle     cyclemanager.CycleManager
	// collects collections of flush callbacks of stores (shards)
	flushCallbacks cyclemanager.CycleCallbacks
	flushCycle     cyclemanager.CycleManager
	// collects collections of vector maintenance callbacks of shards
	vectorCommitLoggerCallbacks cyclemanager.CycleCallbacks
	vectorCommitLoggerCycle     cyclemanager.CycleManager
	// collects collections of vector cleanup callbacks of shards
	vectorTombstoneCleanupCallbacks cyclemanager.CycleCallbacks
	vectorTombstoneCleanupCycle     cyclemanager.CycleManager
	// collects collections of geo properties maintenance callbacks of shards
	geoPropsCommitLoggerCallbacks cyclemanager.CycleCallbacks
	geoPropsCommitLoggerCycle     cyclemanager.CycleManager
	// collects collections of geo properties cleanup callbacks of shards
	geoPropsTombstoneCleanupCallbacks cyclemanager.CycleCallbacks
	geoPropsTombstoneCleanupCycle     cyclemanager.CycleManager
}

func (index *Index) initCycleCallbacks() {
	vectorTombstoneCleanupIntervalSeconds := hnsw.DefaultCleanupIntervalSeconds
	if hnswUserConfig, ok := index.vectorIndexUserConfig.(hnsw.UserConfig); ok {
		vectorTombstoneCleanupIntervalSeconds = hnswUserConfig.CleanupIntervalSeconds
	}

	id := func(elems ...string) string {
		elems = append([]string{"index", index.ID()}, elems...)
		return strings.Join(elems, "/")
	}

	compactionCallbacks := cyclemanager.NewCycleCallbacks(id("compaction"), index.logger, _NUMCPU*2)
	compactionCycle := cyclemanager.New(
		cyclemanager.CompactionCycleTicker(),
		compactionCallbacks.CycleCallback)

	flushCallbacks := cyclemanager.NewCycleCallbacks(id("flush"), index.logger, _NUMCPU*2)
	flushCycle := cyclemanager.New(
		cyclemanager.MemtableFlushCycleTicker(),
		flushCallbacks.CycleCallback)

	vectorCommitLoggerCallbacks := cyclemanager.NewCycleCallbacks(id("vector", "commit_logger"), index.logger, _NUMCPU*2)
	// Previously we had an interval of 10s in here, which was changed to
	// 0.5s as part of gh-1867. There's really no way to wait so long in
	// between checks: If you are running on a low-powered machine, the
	// interval will simply find that there is no work and do nothing in
	// each iteration. However, if you are running on a very powerful
	// machine within 10s you could have potentially created two units of
	// work, but we'll only be handling one every 10s. This means
	// uncombined/uncondensed hnsw commit logs will keep piling up can only
	// be processes long after the initial insert is complete. This also
	// means that if there is a crash during importing a lot of work needs
	// to be done at startup, since the commit logs still contain too many
	// redundancies. So as of now it seems there are only advantages to
	// running the cleanup checks and work much more often.
	//
	// update: switched to dynamic intervals with values between 500ms and 10s
	// introduced to address https://github.com/weaviate/weaviate/issues/2783
	vectorCommitLoggerCycle := cyclemanager.New(
		cyclemanager.HnswCommitLoggerCycleTicker(),
		vectorCommitLoggerCallbacks.CycleCallback)

	vectorTombstoneCleanupCallbacks := cyclemanager.NewCycleCallbacks(id("vector", "tombstone_cleanup"), index.logger, _NUMCPU*2)
	vectorTombstoneCleanupCycle := cyclemanager.New(
		cyclemanager.NewFixedTicker(time.Duration(vectorTombstoneCleanupIntervalSeconds)*time.Second),
		vectorTombstoneCleanupCallbacks.CycleCallback)

	geoPropsCommitLoggerCallbacks := cyclemanager.NewCycleCallbacks(id("geo_props", "commit_logger"), index.logger, _NUMCPU*2)
	geoPropsCommitLoggerCycle := cyclemanager.New(
		cyclemanager.GeoCommitLoggerCycleTicker(),
		geoPropsCommitLoggerCallbacks.CycleCallback)

	geoPropsTombstoneCleanupCallbacks := cyclemanager.NewCycleCallbacks(id("geo_props", "tombstone_cleanup"), index.logger, _NUMCPU*2)
	geoPropsTombstoneCleanupCycle := cyclemanager.New(
		cyclemanager.NewFixedTicker(enthnsw.DefaultCleanupIntervalSeconds*time.Second),
		geoPropsTombstoneCleanupCallbacks.CycleCallback)

	index.cycleCallbacks = &indexCycleCallbacks{
		compactionCallbacks: compactionCallbacks,
		compactionCycle:     compactionCycle,
		flushCallbacks:      flushCallbacks,
		flushCycle:          flushCycle,

		vectorCommitLoggerCallbacks:     vectorCommitLoggerCallbacks,
		vectorCommitLoggerCycle:         vectorCommitLoggerCycle,
		vectorTombstoneCleanupCallbacks: vectorTombstoneCleanupCallbacks,
		vectorTombstoneCleanupCycle:     vectorTombstoneCleanupCycle,

		geoPropsCommitLoggerCallbacks:     geoPropsCommitLoggerCallbacks,
		geoPropsCommitLoggerCycle:         geoPropsCommitLoggerCycle,
		geoPropsTombstoneCleanupCallbacks: geoPropsTombstoneCleanupCallbacks,
		geoPropsTombstoneCleanupCycle:     geoPropsTombstoneCleanupCycle,
	}
}

func (index *Index) initCycleCallbacksNoop() {
	index.cycleCallbacks = &indexCycleCallbacks{
		compactionCallbacks: cyclemanager.NewCycleCallbacksNoop(),
		compactionCycle:     cyclemanager.NewNoop(),
		flushCallbacks:      cyclemanager.NewCycleCallbacksNoop(),
		flushCycle:          cyclemanager.NewNoop(),

		vectorCommitLoggerCallbacks:     cyclemanager.NewCycleCallbacksNoop(),
		vectorCommitLoggerCycle:         cyclemanager.NewNoop(),
		vectorTombstoneCleanupCallbacks: cyclemanager.NewCycleCallbacksNoop(),
		vectorTombstoneCleanupCycle:     cyclemanager.NewNoop(),

		geoPropsCommitLoggerCallbacks:     cyclemanager.NewCycleCallbacksNoop(),
		geoPropsCommitLoggerCycle:         cyclemanager.NewNoop(),
		geoPropsTombstoneCleanupCallbacks: cyclemanager.NewCycleCallbacksNoop(),
		geoPropsTombstoneCleanupCycle:     cyclemanager.NewNoop(),
	}
}
