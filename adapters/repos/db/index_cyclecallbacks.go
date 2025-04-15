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
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type indexCycleCallbacks struct {
	compactionCallbacks    cyclemanager.CycleCallbackGroup
	compactionCycle        cyclemanager.CycleManager
	compactionAuxCallbacks cyclemanager.CycleCallbackGroup
	compactionAuxCycle     cyclemanager.CycleManager

	flushCallbacks cyclemanager.CycleCallbackGroup
	flushCycle     cyclemanager.CycleManager

	vectorCommitLoggerCallbacks     cyclemanager.CycleCallbackGroup
	vectorCommitLoggerCycle         cyclemanager.CycleManager
	vectorTombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup
	vectorTombstoneCleanupCycle     cyclemanager.CycleManager

	geoPropsCommitLoggerCallbacks     cyclemanager.CycleCallbackGroup
	geoPropsCommitLoggerCycle         cyclemanager.CycleManager
	geoPropsTombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup
	geoPropsTombstoneCleanupCycle     cyclemanager.CycleManager
}

func (index *Index) initCycleCallbacks() {
	routinesN := concurrency.TimesNUMCPU(index.Config.CycleManagerRoutinesFactor)

	vectorTombstoneCleanupIntervalSeconds := hnsw.DefaultCleanupIntervalSeconds
	if hnswUserConfig, ok := index.GetVectorIndexConfig("").(hnsw.UserConfig); ok {
		vectorTombstoneCleanupIntervalSeconds = hnswUserConfig.CleanupIntervalSeconds
	}

	id := func(elems ...string) string {
		elems = append([]string{"index", index.ID()}, elems...)
		return strings.Join(elems, "/")
	}

	var compactionCycle cyclemanager.CycleManager
	var compactionCallbacks cyclemanager.CycleCallbackGroup
	var compactionAuxCycle cyclemanager.CycleManager
	var compactionAuxCallbacks cyclemanager.CycleCallbackGroup

	if !index.Config.SeparateObjectsCompactions {
		compactionCallbacks = cyclemanager.NewCallbackGroup(id("compaction"), index.logger, routinesN)
		compactionCycle = cyclemanager.NewManager(
			cyclemanager.CompactionCycleTicker(),
			compactionCallbacks.CycleCallback, index.logger)
		compactionAuxCycle = cyclemanager.NewManagerNoop()
	} else {
		routinesNDiv2 := routinesN / 2
		if routinesNDiv2 < 1 {
			routinesNDiv2 = 1
		}
		compactionCallbacks = cyclemanager.NewCallbackGroup(id("compaction-non-objects"), index.logger, routinesNDiv2)
		compactionCycle = cyclemanager.NewManager(
			cyclemanager.CompactionCycleTicker(),
			compactionCallbacks.CycleCallback, index.logger)
		compactionAuxCallbacks = cyclemanager.NewCallbackGroup(id("compaction-objects"), index.logger, routinesNDiv2)
		compactionAuxCycle = cyclemanager.NewManager(
			cyclemanager.CompactionCycleTicker(),
			compactionAuxCallbacks.CycleCallback, index.logger)
	}

	flushCallbacks := cyclemanager.NewCallbackGroup(id("flush"), index.logger, routinesN)
	flushCycle := cyclemanager.NewManager(
		cyclemanager.MemtableFlushCycleTicker(),
		flushCallbacks.CycleCallback, index.logger)

	vectorCommitLoggerCallbacks := cyclemanager.NewCallbackGroup(id("vector", "commit_logger"), index.logger, routinesN)
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
	vectorCommitLoggerCycle := cyclemanager.NewManager(
		cyclemanager.HnswCommitLoggerCycleTicker(),
		vectorCommitLoggerCallbacks.CycleCallback, index.logger)

	vectorTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup(id("vector", "tombstone_cleanup"), index.logger, routinesN)
	vectorTombstoneCleanupCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(time.Duration(vectorTombstoneCleanupIntervalSeconds)*time.Second),
		vectorTombstoneCleanupCallbacks.CycleCallback, index.logger)

	geoPropsCommitLoggerCallbacks := cyclemanager.NewCallbackGroup(id("geo_props", "commit_logger"), index.logger, routinesN)
	geoPropsCommitLoggerCycle := cyclemanager.NewManager(
		cyclemanager.GeoCommitLoggerCycleTicker(),
		geoPropsCommitLoggerCallbacks.CycleCallback, index.logger)

	geoPropsTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup(id("geo_props", "tombstone_cleanup"), index.logger, routinesN)
	geoPropsTombstoneCleanupCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(hnsw.DefaultCleanupIntervalSeconds*time.Second),
		geoPropsTombstoneCleanupCallbacks.CycleCallback, index.logger)

	index.cycleCallbacks = &indexCycleCallbacks{
		compactionCallbacks:    compactionCallbacks,
		compactionCycle:        compactionCycle,
		compactionAuxCallbacks: compactionAuxCallbacks,
		compactionAuxCycle:     compactionAuxCycle,
		flushCallbacks:         flushCallbacks,
		flushCycle:             flushCycle,

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
		compactionCallbacks:    cyclemanager.NewCallbackGroupNoop(),
		compactionCycle:        cyclemanager.NewManagerNoop(),
		compactionAuxCallbacks: cyclemanager.NewCallbackGroupNoop(),
		compactionAuxCycle:     cyclemanager.NewManagerNoop(),
		flushCallbacks:         cyclemanager.NewCallbackGroupNoop(),
		flushCycle:             cyclemanager.NewManagerNoop(),

		vectorCommitLoggerCallbacks:     cyclemanager.NewCallbackGroupNoop(),
		vectorCommitLoggerCycle:         cyclemanager.NewManagerNoop(),
		vectorTombstoneCleanupCallbacks: cyclemanager.NewCallbackGroupNoop(),
		vectorTombstoneCleanupCycle:     cyclemanager.NewManagerNoop(),

		geoPropsCommitLoggerCallbacks:     cyclemanager.NewCallbackGroupNoop(),
		geoPropsCommitLoggerCycle:         cyclemanager.NewManagerNoop(),
		geoPropsTombstoneCleanupCallbacks: cyclemanager.NewCallbackGroupNoop(),
		geoPropsTombstoneCleanupCycle:     cyclemanager.NewManagerNoop(),
	}
}
