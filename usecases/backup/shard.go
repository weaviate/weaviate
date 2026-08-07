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

package backup

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/entities/backup"
)

const (
	_TimeoutShardCommit = 20 * time.Second
)

type reqState struct {
	Starttime time.Time
	ID        string
	Status    backup.Status
	// Err is why the operation ended, for the statuses that need one. It lives
	// only as long as the slot; see [backupStat.failedReason] for what a poll
	// arriving after that reads.
	Err            string
	Path           string
	OverrideBucket string
	OverridePath   string
}

type backupStat struct {
	sync.Mutex
	reqState

	// failedID and failedReason outlive the slot itself, for the one failure
	// that leaves nothing else to read: the slot is released as soon as the
	// operation returns, and a later poll is answered from the descriptor on
	// the backend, which does not exist when writing it is what failed.
	failedID     string
	failedReason string
}

func (s *backupStat) get() reqState {
	s.Lock()
	defer s.Unlock()
	return s.reqState
}

// renew state if and only it is not in use
// it returns "" in case of success and current id in case of failure
func (s *backupStat) renew(id string, path string, overrideBucket, overridePath string) string {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != "" {
		return s.reqState.ID
	}
	s.reqState.ID = id
	s.reqState.Path = path
	s.reqState.OverrideBucket = overrideBucket
	s.reqState.OverridePath = overridePath
	s.reqState.Starttime = time.Now().UTC()
	s.reqState.Status = backup.Started
	s.reqState.Err = ""
	if s.failedID == id {
		// A retry under the same id: the earlier failure is no longer the
		// answer to a poll for it.
		s.failedID, s.failedReason = "", ""
	}
	return ""
}

func (s *backupStat) reset() {
	s.Lock()
	s.clear()
	s.Unlock()
}

// clear must be called with the lock held.
func (s *backupStat) clear() {
	s.reqState.ID = ""
	s.reqState.Path = ""
	s.reqState.Status = ""
	s.reqState.Err = ""
	s.reqState.OverrideBucket = ""
	s.reqState.OverridePath = ""
}

// resetIfCancelled clears the slot only if id owns it and its status is
// cancelled; an operation still running must keep its slot. Check-and-clear
// happens under one lock so a concurrent renew can't be lost. Reports whether it cleared.
func (s *backupStat) resetIfCancelled(id string) bool {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != id {
		return false
	}
	if s.reqState.Status != backup.Cancelling && s.reqState.Status != backup.Cancelled {
		return false
	}
	s.clear()
	return true
}

// resetIfOwned clears the slot only if id still holds it. An operation whose
// slot was already handed to a newer one must not free the newcomer's claim:
// the slot is the node's busy signal, so a false idle lets a runtime-reindex
// start on top of a live backup or restore. Check-and-clear happens under one
// lock so a concurrent renew can't be lost. Reports whether it cleared.
func (s *backupStat) resetIfOwned(id string) bool {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != id {
		return false
	}
	s.clear()
	return true
}

// setFailed ends the operation as failed together with the reason. Failed with
// no reason is worse than useless to a poller: the coordinator latches whatever
// a participant reports and stops asking, so an empty reason becomes the
// permanent answer for a failure that does have one.
func (s *backupStat) setFailed(reason string) {
	s.Lock()
	defer s.Unlock()
	if s.reqState.Status == backup.Cancelled {
		return
	}
	s.reqState.Status = backup.Failed
	s.reqState.Err = reason
	if reason == "" {
		return
	}
	s.failedID = s.reqState.ID
	s.failedReason = reason
}

// rememberedFailure reports why the operation with this id ended failed, for
// polls arriving after the slot was released. Absent for anything that did not
// end failed with a reason. The id has to match: a poll for one backup must
// never be answered with what happened to another.
func (s *backupStat) rememberedFailure(id string) (string, bool) {
	s.Lock()
	defer s.Unlock()
	if id == "" || s.failedID != id {
		return "", false
	}
	return s.failedReason, true
}

func (s *backupStat) set(st backup.Status) {
	s.Lock()
	defer s.Unlock()
	// Cancelled is terminal - don't allow overwriting
	if s.reqState.Status == backup.Cancelled {
		return
	}
	s.reqState.Status = st
}

// setIfOwned writes the status only if id still holds the slot, the write half
// of the same ownership rule as [backupStat.resetIfOwned]. A caller that does
// not derive id from the slot itself — a cancel, which takes it from object
// storage — would otherwise stamp whichever operation happens to hold it, and a
// slot reading Cancelled makes coordinator.commit abort the operation as
// "cancelled externally". Reports whether it wrote.
func (s *backupStat) setIfOwned(id string, st backup.Status) bool {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != id {
		return false
	}
	// Cancelled is terminal - don't allow overwriting
	if s.reqState.Status == backup.Cancelled {
		return false
	}
	s.reqState.Status = st
	return true
}

// shardSyncChan makes sure that a backup operation is mutually exclusive.
// It also contains the channel used to communicate with the coordinator.
type shardSyncChan struct {
	// lastOp makes sure backup operations are mutually exclusive
	lastOp backupStat

	// waitingForCoordinatorToCommit use while waiting for the coordinator to take the next action
	waitingForCoordinatorToCommit atomic.Bool
	//  coordChan used to communicate with the coordinator
	coordChan chan interface{}

	// lastAsyncError used for debugging when no metadata is created
	lastAsyncError error
}

// waitForCoordinator to confirm or to abort previous operation
func (c *shardSyncChan) waitForCoordinator(d time.Duration, id string) error {
	defer c.waitingForCoordinatorToCommit.Store(false)
	if d == 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timed out waiting for coordinator to commit")
		case v := <-c.coordChan:
			switch v := v.(type) {
			case AbortRequest:
				if v.ID == id {
					return fmt.Errorf("coordinator aborted operation")
				}
			case StatusRequest:
				if v.ID == id {
					return nil
				}
			}
		}
	}
}

// withCancellation return a new context which will be cancelled if the coordinator
// want to abort the commit phase
func (c *shardSyncChan) withCancellation(ctx context.Context, id string, done chan struct{}, logger logrus.FieldLogger) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	enterrors.GoWrapper(func() {
		defer cancel()
		for {
			select {
			case v := <-c.coordChan:
				switch v := v.(type) {
				case AbortRequest:
					if v.ID == id {
						return
					}
					// Log unexpected abort request with different ID - this shouldn't happen
					// since OnAbort checks the ID before sending, but log for debugging
					if logger != nil {
						logger.WithFields(map[string]interface{}{
							"action":      "withCancellation",
							"expected_id": id,
							"received_id": v.ID,
						}).Warn("received abort request for different backup ID, ignoring")
					}
				}
			case <-done: // caller is done
				return
			}
		}
	}, logger)
	return ctx
}

// OnCommit will be triggered when the coordinator confirms the execution of a previous operation
func (c *shardSyncChan) OnCommit(ctx context.Context, req *StatusRequest) error {
	st := c.lastOp.get()
	if st.ID == req.ID && c.waitingForCoordinatorToCommit.Load() {
		c.coordChan <- *req
		return nil
	}
	return fmt.Errorf("shard has abandon backup operation")
}

// Abort tells a node to abort the previous backup operation
func (c *shardSyncChan) OnAbort(_ context.Context, req *AbortRequest) error {
	st := c.lastOp.get()
	if st.ID == req.ID {
		c.coordChan <- *req
		return nil
	}
	// No active operation with this ID - this is not an error, the operation may have
	// already completed or never started on this node. Return nil for idempotency.
	return nil
}
