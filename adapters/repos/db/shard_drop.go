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
	dropStart := time.Now()
	logStep := func(step string, started time.Time, extra logrus.Fields) {
		f := logrus.Fields{
			"action":     "drop_step_timing",
			"layer":      "shard",
			"step":       step,
			"class":      s.class.Class,
			"shard":      s.name,
			"elapsed_ms": time.Since(started).Milliseconds(),
		}
		for k, v := range extra {
			f[k] = v
		}
		s.index.logger.WithFields(f).Info("drop step completed")
	}
	defer func() {
		logStep("shard_drop_total", dropStart, nil)
	}()

	s.shutCtxCancel(fmt.Errorf("drop %q", s.ID()))
	s.reindexer.Stop(s, fmt.Errorf("shard drop"))

	s.metrics.DeleteShardLabels(s.index.Config.ClassName.String(), s.name)
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
	vqStart := time.Now()
	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Drop(ctx); err != nil {
			return fmt.Errorf("close queue of vector %q at %s: %w", targetVector, s.path(), err)
		}
		return nil
	})
	logStep("for_each_vector_queue_drop", vqStart, nil)
	if err != nil {
		return err
	}

	gqStart := time.Now()
	err = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err = queue.Drop(ctx); err != nil {
			return fmt.Errorf("close geo queue of prop %q at %s: %w", propName, s.path(), err)
		}
		return nil
	})
	logStep("for_each_geo_queue_drop", gqStart, nil)
	if err != nil {
		return err
	}

	viStart := time.Now()
	err = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		if err = index.Drop(ctx, keepFiles); err != nil {
			return fmt.Errorf("remove vector index of vector %q at %s: %w", targetVector, s.path(), err)
		}
		return nil
	})
	logStep("for_each_vector_index_drop", viStart, nil)
	if err != nil {
		return err
	}

	// unregister all callbacks at once, in parallel
	ccStart := time.Now()
	if err = cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.compactionAuxCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx); err != nil {
		logStep("combined_callback_ctrl_unregister", ccStart, logrus.Fields{"err": err.Error()})
		return err
	}
	logStep("combined_callback_ctrl_unregister", ccStart, nil)

	storeStart := time.Now()
	if err = s.store.Shutdown(ctx); err != nil {
		logStep("store_shutdown", storeStart, logrus.Fields{"err": err.Error()})
		return errors.Wrap(err, "stop lsmkv store")
	}
	logStep("store_shutdown", storeStart, nil)

	rmLSMStart := time.Now()
	if _, err = os.Stat(s.pathLSM()); err == nil && !keepFiles {
		err := os.RemoveAll(s.pathLSM())
		if err != nil {
			logStep("os_remove_all_lsm", rmLSMStart, logrus.Fields{"err": err.Error()})
			return errors.Wrapf(err, "remove lsm store at %s", s.pathLSM())
		}
	}
	logStep("os_remove_all_lsm", rmLSMStart, nil)

	// delete indexcount
	counterStart := time.Now()
	err = s.counter.Drop(keepFiles)
	logStep("counter_drop", counterStart, nil)
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.path())
	}

	// delete version
	versionerStart := time.Now()
	err = s.versioner.Drop(keepFiles)
	logStep("versioner_drop", versionerStart, nil)
	if err != nil {
		return errors.Wrapf(err, "remove version at %s", s.path())
	}

	// delete property length tracker
	pltStart := time.Now()
	err = s.GetPropertyLengthTracker().Drop(keepFiles)
	logStep("property_length_tracker_drop", pltStart, nil)
	if err != nil {
		return errors.Wrapf(err, "remove prop length tracker at %s", s.path())
	}

	piStart := time.Now()
	s.propertyIndicesLock.Lock()
	err = s.propertyIndices.DropAll(ctx, keepFiles)
	s.propertyIndicesLock.Unlock()
	logStep("property_indices_drop_all", piStart, nil)
	if err != nil {
		return errors.Wrapf(err, "remove property specific indices at %s", s.path())
	}

	// remove shard dir
	rmShardStart := time.Now()
	if !keepFiles {
		if err := os.RemoveAll(s.path()); err != nil {
			logStep("os_remove_all_shard_dir", rmShardStart, logrus.Fields{"err": err.Error()})
			return fmt.Errorf("delete shard dir: %w", err)
		}
	}
	logStep("os_remove_all_shard_dir", rmShardStart, nil)

	// Only update metrics if the shard was properly registered
	if s.metricsRegistered.Load() {
		s.metrics.baseMetrics.DeleteLoadedShard()
	}

	s.index.logger.WithFields(logrus.Fields{
		"action": "drop_shard",
		"class":  s.class.Class,
		"shard":  s.name,
	}).Debug("shard successfully dropped")

	return nil
}
