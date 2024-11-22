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
	compactionCallbacks        cyclemanager.CycleCallbackGroup
	compactionCallbacksCtrl    cyclemanager.CycleCallbackCtrl
	compactionAuxCallbacks     cyclemanager.CycleCallbackGroup
	compactionAuxCallbacksCtrl cyclemanager.CycleCallbackCtrl

	flushCallbacks     cyclemanager.CycleCallbackGroup
	flushCallbacksCtrl cyclemanager.CycleCallbackCtrl
}

func (s *Store) initCycleCallbacks(shardCompactionCallbacks, shardCompactionAuxCallbacks,
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

	var compactionCallbacks cyclemanager.CycleCallbackGroup
	var compactionCallbacksCtrl cyclemanager.CycleCallbackCtrl
	var compactionAuxCallbacks cyclemanager.CycleCallbackGroup
	var compactionAuxCallbacksCtrl cyclemanager.CycleCallbackCtrl

	if shardCompactionAuxCallbacks == nil {
		compactionId := id("compaction")
		compactionCallbacks = cyclemanager.NewCallbackGroup(compactionId, s.logger, 1)
		compactionCallbacksCtrl = shardCompactionCallbacks.Register(
			compactionId, compactionCallbacks.CycleCallback)
		compactionAuxCallbacksCtrl = cyclemanager.NewCallbackCtrlNoop()
	} else {
		compactionId := id("compaction-non-objects")
		compactionCallbacks = cyclemanager.NewCallbackGroup(compactionId, s.logger, 1)
		compactionCallbacksCtrl = shardCompactionCallbacks.Register(
			compactionId, compactionCallbacks.CycleCallback)
		compactionAuxId := id("compaction-objects")
		compactionAuxCallbacks = cyclemanager.NewCallbackGroup(compactionAuxId, s.logger, 1)
		compactionAuxCallbacksCtrl = shardCompactionAuxCallbacks.Register(
			compactionAuxId, compactionAuxCallbacks.CycleCallback)
	}

	flushId := id("flush")
	flushCallbacks := cyclemanager.NewCallbackGroup(flushId, s.logger, 1)
	flushCallbacksCtrl := shardFlushCallbacks.Register(
		flushId, flushCallbacks.CycleCallback)

	s.cycleCallbacks = &storeCycleCallbacks{
		compactionCallbacks:        compactionCallbacks,
		compactionCallbacksCtrl:    compactionCallbacksCtrl,
		compactionAuxCallbacks:     compactionAuxCallbacks,
		compactionAuxCallbacksCtrl: compactionAuxCallbacksCtrl,

		flushCallbacks:     flushCallbacks,
		flushCallbacksCtrl: flushCallbacksCtrl,
	}
}
