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

package lsmkv

import (
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type storeCycleCallbacks struct {
	compactionObjectsCallbacks       cyclemanager.CycleCallbackGroup
	compactionObjectCallbacksCtrl    cyclemanager.CycleCallbackCtrl
	compactionNonObjectsCallbacks    cyclemanager.CycleCallbackGroup
	compactionNonObjectCallbacksCtrl cyclemanager.CycleCallbackCtrl

	flushCallbacks     cyclemanager.CycleCallbackGroup
	flushCallbacksCtrl cyclemanager.CycleCallbackCtrl
}

func (s *Store) initCycleCallbacks(shardCompactionObjectsCallbacks, shardCompactionNonObjectsCallbacks,
	shardFlushCallbacks cyclemanager.CycleCallbackGroup,
) {
	id := func(elems ...string) string {
		path, err := filepath.Rel(s.dir, s.rootDir)
		if err != nil {
			path = s.dir
		}
		elems = append([]string{"store"}, elems...)
		elems = append(elems, path)
		return strings.Join(elems, "/")
	}

	compactionObjectsId := id("compaction-objects")
	compactionObjectsCallbacks := cyclemanager.NewCallbackGroup(compactionObjectsId, s.logger, 1)
	compactionObjectsCallbacksCtrl := shardCompactionObjectsCallbacks.Register(
		compactionObjectsId, compactionObjectsCallbacks.CycleCallback)

	compactionNonObjectsId := id("compaction-non-objects")
	compactionNonObjectsCallbacks := cyclemanager.NewCallbackGroup(compactionNonObjectsId, s.logger, 1)
	compactionNonObjectsCallbacksCtrl := shardCompactionNonObjectsCallbacks.Register(
		compactionNonObjectsId, compactionNonObjectsCallbacks.CycleCallback)

	flushId := id("flush")
	flushCallbacks := cyclemanager.NewCallbackGroup(flushId, s.logger, 1)
	flushCallbacksCtrl := shardFlushCallbacks.Register(
		flushId, flushCallbacks.CycleCallback)

	s.cycleCallbacks = &storeCycleCallbacks{
		compactionObjectsCallbacks:       compactionObjectsCallbacks,
		compactionObjectCallbacksCtrl:    compactionObjectsCallbacksCtrl,
		compactionNonObjectsCallbacks:    compactionNonObjectsCallbacks,
		compactionNonObjectCallbacksCtrl: compactionNonObjectsCallbacksCtrl,

		flushCallbacks:     flushCallbacks,
		flushCallbacksCtrl: flushCallbacksCtrl,
	}
}
