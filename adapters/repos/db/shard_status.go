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
	"errors"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
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
	s.migrationMirrors.stopCountingHeld()

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
	if paused, _ := s.index.nodeOverlay().StorePaused(s.phase); paused {
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

// refuseWrite fails a write the node overlay refuses; a user's READONLY is refuseUserReadOnly's.
func (s *Shard) refuseWrite() error {
	return s.index.nodeOverlay().RefuseWrite(s.phase)
}

// syncStorePause re-reads every condition, so lifting one cannot resume a store another still pauses.
func (s *Shard) syncStorePause() error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	next := s.phase.Status()
	if next == storagestate.StatusShutdown {
		return nil
	}

	paused, tempPaused := s.index.nodeOverlay().StorePaused(s.phase)
	if err := s.store.PauseBackgroundIOBy(func(bucket string) bool {
		return paused && (tempPaused || !strings.HasSuffix(bucket, schema.InternalTempSuffix))
	}); err != nil {
		return err
	}

	next = storagestate.Status(s.index.nodeOverlay().Report(s.index.Config.ClassName.String(), s.name, s.phase, next).Status)
	if prev := s.countedStatus; prev != "" && prev != next.String() {
		lvl := logrus.DebugLevel
		if next == storagestate.StatusReadOnly {
			lvl = logrus.WarnLevel
		}
		s.index.logger.WithFields(logrus.Fields{
			"action": "update_shard_status",
			"class":  s.index.Config.ClassName,
			"shard":  s.name,
			"status": next.String(),
			"prev":   prev,
		}).Log(lvl, "shard status changed")
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
	// a closed store is the only failure, and it takes no writes
	_ = s.store.PauseBackgroundIO(false)
}

// setReindexing(false) also ends the swap: a reindex that fails mid-swap gives the store back.
func (s *Shard) setReindexing(reindexing bool) error {
	if reindexing {
		s.phase.SetCondition(clusterschema.PhaseReindexing, true)
	} else {
		s.phase.SetCondition(clusterschema.PhaseReindexing|clusterschema.PhaseReindexSwapping, false)
	}
	return s.syncStorePause()
}

// setReindexSwapping pauses the reindex's temp buckets too, so they can be renamed into place.
func (s *Shard) setReindexSwapping() error {
	s.phase.SetCondition(clusterschema.PhaseReindexSwapping, true)
	return s.syncStorePause()
}

func (s *Shard) setMigrationHeld(held bool) error {
	s.phase.SetCondition(clusterschema.PhaseMigrationHeld, held)
	return s.syncStorePause()
}

// retryHeldMigrations starts the arm and refuses at once: a status change can run inside a RAFT apply,
// which must never wait on the disk.
// A shutdown waits for the arm's shard reference, and a drop waits about 30 s for it.
func (s *Shard) retryHeldMigrations() error {
	if len(s.migrationMirrors.heldUnarmed()) == 0 || s.shut.Load() {
		return nil
	}
	release, err := s.preventShutdown()
	if err != nil {
		return s.heldReadOnlyErr(fmt.Errorf("the shard is shutting down: %w", err))
	}
	if !s.liftRetry.TryLock() {
		release()
		return s.heldReadOnlyErr(errors.New("a recovery retry is already running"))
	}
	enterrors.GoWrapper(func() {
		defer release()
		defer s.liftRetry.Unlock()
		if err := s.rearmHeld(); err != nil {
			s.index.logger.WithField("shard", s.name).Errorf("recovery retry of a held shard failed: %v", err)
		}
	}, s.index.logger)
	return s.heldReadOnlyErr(errors.New("a recovery retry started; this node's log names a failure"))
}

// rearmHeld reads the held set under liftRetry, because an earlier lift's arm may have armed or stamped it since.
// It arms only units ClaimUnits claimed, which a cleanup's seal waits for, so a teardown cannot remove what the arm re-creates.
func (s *Shard) rearmHeld() error {
	held := s.migrationMirrors.heldUnarmed()
	if len(held) == 0 {
		return nil
	}
	claimed, skipped, releaseClaims := s.reindexer.ClaimUnits(held)
	defer releaseClaims()
	unarmed, err := s.armRecoveredMigrations(context.WithoutCancel(s.shutCtx), claimed)
	ec := errorcompounder.New()
	ec.Add(err)
	if len(skipped) > 0 {
		ec.Add(fmt.Errorf("a teardown holds the unit of %d migration(s); set the status again once it ends", len(skipped)))
	}
	if err != nil || len(skipped) > 0 {
		// Buckets the arm opened pause again before the hold changes, so no lift races the reset.
		ec.Add(s.syncStorePause())
		s.migrationMirrors.holdUnarmed(append(unarmed, skipped...))
	} else {
		s.migrationMirrors.releaseHeld()
		ec.Add(s.setMigrationHeld(false))
	}
	return ec.ToError()
}

func (s *Shard) heldReadOnlyErr(cause error) error {
	return fmt.Errorf("shard %q: a reindex migration's double-write mirror is not armed and "+
		"cannot be stamped Unmirrored: %w", s.name, cause)
}

func (i *Index) nodeOverlay() *clusterschema.NodeOverlay {
	if i.db == nil {
		return nil
	}
	return i.db.nodeOverlay
}

func (i *Index) refuseUserReadOnly(shardName string) error {
	return i.nodeOverlay().RefuseClientWrite(i.Config.ClassName.String(), shardName)
}

// updateShardStatus is the store's part of a PUT on a shard's status: a READY retries the reindex
// migrations holding a loaded shard read-only. It runs in the RAFT apply, so it never loads a shard or
// waits on a loading one.
func (i *Index) updateShardStatus(_ context.Context, shardName, targetStatus string) error {
	if strings.EqualFold(targetStatus, storagestate.StatusReadOnly.String()) {
		return nil
	}
	loaded := i.shards.Load(shardName)
	shard, _ := loaded.(*Shard)
	if lazy, ok := asLazyLoadShard(loaded); ok {
		shard = lazy.currentShard()
	}
	if shard == nil {
		return nil
	}
	return shard.retryHeldMigrations()
}

// reportedStatus is a shard's status on this node and the conditions behind it; a nil shard is cold.
func (i *Index) reportedStatus(shardName string, shard ShardLike) storagestate.Report {
	status, phase := storagestate.StatusLazyLoading, (*clusterschema.ShardPhase)(nil)
	if shard != nil {
		status = shard.GetStatus()
		if s, ok := shard.(*Shard); ok {
			phase = s.phase
		} else if lazy, ok := asLazyLoadShard(shard); ok && lazy.currentShard() != nil {
			phase = lazy.currentShard().phase
		}
	}
	return i.nodeOverlay().Report(i.Config.ClassName.String(), shardName, phase, status)
}
