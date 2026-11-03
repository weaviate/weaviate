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
	"errors"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
)

const (
	statusReasonResourcePressure = "resource pressure"
	statusReasonManualUpdate     = "manually set by user"
)

func (s *Shard) registerCountedStatus(status storagestate.Status) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.index.metrics.UpdateShardStatus("", status.String())
	s.countedStatus = status.String()
}

// moveCountedStatusUnlocked follows a status change in the gauge; an uncounted
// (released) shard stays uncounted.
func (s *Shard) moveCountedStatusUnlocked(next storagestate.Status) {
	if s.countedStatus == "" || s.countedStatus == next.String() {
		return
	}

	s.index.metrics.UpdateShardStatus(s.countedStatus, next.String())
	s.countedStatus = next.String()
}

// releaseCountedStatus is idempotent and safe on a never-counted shard.
func (s *Shard) releaseCountedStatus() {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	if s.countedStatus == "" {
		return
	}

	s.index.metrics.UpdateShardStatus(s.countedStatus, "")
	s.countedStatus = ""
}

func (s *Shard) GetStatus() storagestate.Status {
	phase := s.phase.Status()
	if phase != storagestate.StatusReady {
		return phase
	}
	if s.backgroundIOPaused() {
		return storagestate.StatusReadOnly
	}
	if !s.hasAnyVectorIndex() {
		return storagestate.StatusReady
	}

	status := storagestate.StatusReady
	_ = s.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
		if queue.Size() > 0 {
			status = storagestate.StatusIndexing
		}
		return nil
	})
	_ = s.ForEachGeoQueue(func(_ string, queue *VectorIndexQueue) error {
		if queue.Size() > 0 {
			status = storagestate.StatusIndexing
		}
		return nil
	})
	return status
}

// refuseWhilePaused fails a write while the store is paused; a user's READONLY is refuseUserReadOnly's.
func (s *Shard) refuseWhilePaused() error {
	if err := s.index.refuseResourcePressure(); err != nil {
		return err
	}
	if s.phase.Reindexing() {
		return storagestate.ErrStatusReadOnly
	}
	if s.phase.Transferring() {
		return errTenantInTransition
	}
	return nil
}

func (s *Shard) backgroundIOPaused() bool {
	return s.refuseWhilePaused() != nil
}

// syncStorePause re-reads every condition, so lifting one cannot resume a store another still pauses.
func (s *Shard) syncStorePause() error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	next := s.phase.Status()
	if next == storagestate.StatusShutdown {
		return nil
	}

	paused := s.backgroundIOPaused()
	// A reindex writes its temp buckets while it holds the rest of the store.
	reindexOnly := s.phase.Reindexing() && !s.phase.Transferring() && s.index.refuseResourcePressure() == nil
	if err := s.store.PauseBackgroundIOExcept(paused, func(bucket string) bool {
		return reindexOnly && strings.HasSuffix(bucket, schema.InternalTempSuffix)
	}); err != nil {
		return err
	}
	if !paused && s.vectorConfigStale.CompareAndSwap(true, false) {
		enterrors.GoWrapper(s.applyRefusedVectorConfig, s.index.logger)
	}

	if next == storagestate.StatusReady && (paused || s.index.userReadOnly(s.name)) {
		next = storagestate.StatusReadOnly
	}
	s.moveCountedStatusUnlocked(next)
	return nil
}

// markShutdown lifts any pause for good: a store going down takes what is in flight.
func (s *Shard) markShutdown() {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.phase.Set(storagestate.StatusShutdown)
	s.moveCountedStatusUnlocked(storagestate.StatusShutdown)
	if err := s.store.PauseBackgroundIO(false); err != nil && !errors.Is(err, lsmkv.ErrAlreadyClosed) {
		s.index.logger.WithField("action", "update_shard_status").
			Errorf("resume background I/O for shutdown: shard %q: %v", s.name, err)
	}
}

func (s *Shard) dropPhase() {
	s.index.nodeOverlay().DropShardPhase(s.index.Config.ClassName.String(), s.name, s.phase)
}

func (s *Shard) setReindexing(reindexing bool) error {
	s.phase.SetReindexing(reindexing)
	return s.syncStorePause()
}

func (i *Index) refuseResourcePressure() error {
	if i.db.resourcePressureReadOnly() {
		return storagestate.ErrStatusReadOnlyWithReason(statusReasonResourcePressure)
	}
	return nil
}

func (i *Index) nodeOverlay() *clusterschema.NodeOverlay {
	if i.db == nil {
		return nil
	}
	return i.db.nodeOverlay
}

// refuseUserReadOnly is for client writes only: migrations and repair skip it.
func (i *Index) refuseUserReadOnly(shardName string) error {
	if i.userReadOnly(shardName) {
		return storagestate.ErrStatusReadOnlyWithReason(statusReasonManualUpdate)
	}
	return nil
}

func (i *Index) userReadOnly(shardName string) bool {
	return i.nodeOverlay().ReadOnly(i.Config.ClassName.String(), shardName)
}

func (i *Index) reportedStatus(shardName string, shard ShardLike) string {
	status := shard.GetStatus()
	if (status == storagestate.StatusReady || status == storagestate.StatusIndexing) && i.userReadOnly(shardName) {
		return storagestate.StatusReadOnly.String()
	}
	return status.String()
}
