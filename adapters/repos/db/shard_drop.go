//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

// IMPORTANT:
// Be advised there exists LazyLoadShard::drop() implementation intended
// to drop shard that was not loaded (instantiated) yet.
// It deletes shard by performing required actions and removing entire shard directory.
// If there is any action that needs to be performed beside files/dirs being removed
// from shard directory, it needs to be reflected as well in LazyLoadShard::drop()
// method to keep drop behaviour consistent.
func (s *Shard) drop() (err error) {
	s.index.logger.WithFields(logrus.Fields{
		"action": "drop_shard",
		"class":  s.class.Class,
		"shard":  s.name,
	}).Debug("dropping shard")

	s.metrics.DeleteShardLabels(s.index.Config.ClassName.String(), s.name)
	s.metrics.baseMetrics.StartUnloadingShard()
	s.replicationMap.clear()

	ec := errorcompounder.New()

	s.reindexer.Stop(s, fmt.Errorf("shard drop"))

	s.clearDimensionMetrics() // not deleted in s.metrics.DeleteShardLabels

	s.mayStopAsyncReplication()

	s.haltForTransferMux.Lock()
	if s.haltForTransferCancel != nil {
		s.haltForTransferCancel()
	}
	s.haltForTransferMux.Unlock()

	ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancel()

	// unregister all callbacks at once, in parallel
	if err = cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.compactionAuxCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx); err != nil {
		ec.AddWrap(err, fmt.Sprintf("unregister cycle callbacks while dropping shard %q", s.name))
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Drop(); err != nil {
			return fmt.Errorf("close queue of vector %q at %s: %w", targetVector, s.path(), err)
		}
		return nil
	})
	if err != nil {
		ec.AddWrap(err, fmt.Sprintf("drop vector queue of vector at %s", s.path()))
	}

	err = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		if err = index.Drop(ctx); err != nil {
			return fmt.Errorf("remove vector index of vector %q at %s: %w", targetVector, s.path(), err)
		}
		return nil
	})
	if err != nil {
		ec.AddWrap(err, fmt.Sprintf("remove vector index of vector at %s", s.path()))
	}

	// delete property length tracker
	err = s.GetPropertyLengthTracker().Drop()
	if err != nil {
		ec.AddWrap(err, fmt.Sprintf("remove prop length tracker at %s", s.path()))
	}

	s.propertyIndicesLock.Lock()
	err = s.propertyIndices.DropAll(ctx)
	s.propertyIndicesLock.Unlock()
	if err != nil {
		ec.AddWrap(err, fmt.Sprintf("remove property specific indices at %s", s.path()))
	}

	if err = s.store.Shutdown(ctx); err != nil {
		ec.AddWrap(err, fmt.Sprintf("stop lsmkv store at %s", s.path()))
	}

	if _, err = os.Stat(s.pathLSM()); err == nil {
		err := os.RemoveAll(s.pathLSM())
		if err != nil {
			ec.AddWrap(err, fmt.Sprintf("remove lsm store at %s", s.pathLSM()))
		}
	}

	// delete indexcount
	err = s.counter.Drop()
	if err != nil {
		ec.AddWrap(err, fmt.Sprintf("remove indexcount at %s", s.path()))
	}

	// delete version
	err = s.versioner.Drop()
	if err != nil {
		ec.AddWrap(err, fmt.Sprintf("remove version at %s", s.path()))
	}

	// remove shard dir
	if err := os.RemoveAll(s.path()); err != nil {
		ec.AddWrap(err, fmt.Sprintf("delete shard dir at %s", s.path()))
	}

	s.metrics.baseMetrics.FinishUnloadingShard()

	s.index.logger.WithFields(logrus.Fields{
		"action": "drop_shard",
		"class":  s.class.Class,
		"shard":  s.name,
	}).Debug("shard dropped")

	return ec.ToError()
}
