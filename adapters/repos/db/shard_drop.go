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
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// IMPORTANT:
// Be advised there exists LazyLoadShard::drop() implementation intended
// to drop shard that was not loaded (instantiated) yet.
// It deletes shard by performing required actions and removing entire shard directory.
// If there is any action that needs to be performed beside files/dirs being removed
// from shard directory, it needs to be reflected as well in LazyLoadShard::drop()
// method to keep drop behaviour consistent.
// If keepFiles==true, all files on disk are kept, only in-memory structures are removed. This is used to allow backups
// to complete before the files are deleted.
func (s *Shard) drop(keepFiles bool) (err error) {
	s.shutCtxCancel(fmt.Errorf("drop %q", s.ID()))
	s.reindexer.Stop(s, fmt.Errorf("shard drop"))

	s.metrics.DeleteShardLabels(s.index.Config.ClassName.String(), s.name)
	s.metrics.baseMetrics.StartUnloadingShard()
	s.replicationMap.clear()

	s.index.logger.WithFields(logrus.Fields{
		"action": "drop_shard",
		"class":  s.class.Class,
		"shard":  s.name,
	}).Debug("dropping shard")

	s.clearDimensionMetrics() // not deleted in s.metrics.DeleteShardLabels

	s.mayStopAsyncReplication()

	s.haltForTransferMux.Lock()
	if s.haltForTransferCancel != nil {
		s.haltForTransferCancel()
	}
	s.haltForTransferMux.Unlock()

	ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancel()

	// queues need to be closed first to make sure they are not writing anymore
	// to their associated vector index, as they might still be using the store
	// and other resources we are about to drop.
	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Drop(ctx); err != nil {
			return fmt.Errorf("close queue of vector %q at %s: %w", targetVector, s.path(), err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		if err = index.Drop(ctx, keepFiles); err != nil {
			return fmt.Errorf("remove vector index of vector %q at %s: %w", targetVector, s.path(), err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Unregister cycle callbacks for THIS shard with an already-cancelled
	// ctx — the shard is being deleted, there is no reason to wait for
	// in-flight compaction/flush callbacks on it. The mutation (marking
	// shouldAbort=true and removing the callback from the cyclemanager's
	// dispatch list) still applies; only the wait is skipped. See
	// weaviate/0-weaviate-issues#250.
	//
	// Iterate the ctrls one by one so we can distinguish the expected
	// context.Canceled (from the just-cancelled ctx) from real errors.
	// NewCombinedCallbackCtrl coalesces via errorcompounder, which flattens
	// wrapping and would defeat errors.Is.
	cycleCancelCtx, cycleCancel := context.WithCancel(context.Background())
	cycleCancel()
	for _, ctrl := range []cyclemanager.CycleCallbackCtrl{
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.compactionAuxCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	} {
		if err := ctrl.Unregister(cycleCancelCtx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("unregister shard cycle callback: %w", err)
		}
	}

	if err = s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	if _, err = os.Stat(s.pathLSM()); err == nil && !keepFiles {
		err := os.RemoveAll(s.pathLSM())
		if err != nil {
			return errors.Wrapf(err, "remove lsm store at %s", s.pathLSM())
		}
	}
	// delete indexcount
	err = s.counter.Drop(keepFiles)
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.path())
	}

	// delete version
	err = s.versioner.Drop(keepFiles)
	if err != nil {
		return errors.Wrapf(err, "remove version at %s", s.path())
	}

	// delete property length tracker
	err = s.GetPropertyLengthTracker().Drop(keepFiles)
	if err != nil {
		return errors.Wrapf(err, "remove prop length tracker at %s", s.path())
	}

	s.propertyIndicesLock.Lock()
	err = s.propertyIndices.DropAll(ctx, keepFiles)
	s.propertyIndicesLock.Unlock()
	if err != nil {
		return errors.Wrapf(err, "remove property specific indices at %s", s.path())
	}

	// remove shard dir
	if !keepFiles {
		if err := os.RemoveAll(s.path()); err != nil {
			return fmt.Errorf("delete shard dir: %w", err)
		}
	}

	s.metrics.baseMetrics.FinishUnloadingShard()

	s.index.logger.WithFields(logrus.Fields{
		"action": "drop_shard",
		"class":  s.class.Class,
		"shard":  s.name,
	}).Debug("shard successfully dropped")

	return nil
}
