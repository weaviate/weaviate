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
	// only as long as the slot; a poll arriving after that is answered from
	// backupStat.rememberedFailure.
	Err            string
	Path           string
	OverrideBucket string
	OverridePath   string
}

type backupStat struct {
	sync.Mutex
	reqState

	// generation counts the claims this slot has handed out. Backup ids are
	// user-supplied and reusable — cancelling an operation and retrying it
	// under the same id is a normal flow — so the id alone does not identify a
	// claim. renew gives the claiming operation the generation of its own
	// claim, and that is what the release paths match on.
	generation uint64

	// rememberedFailureID and rememberedFailureReason outlive the slot itself,
	// for the one failure that leaves nothing else to read: the slot is
	// released as soon as the operation returns, and a later poll is answered
	// from the descriptor on the backend, which does not exist when writing it
	// is what failed. Memory only: after a restart such a poll is back to
	// being answered with "metadata not found".
	rememberedFailureID     string
	rememberedFailureReason string
}

// failureWithoutReason stands in for a failure reported with no text at all,
// which reads to a poller as no failure.
const failureWithoutReason = "backup failed without a reported reason"

func (s *backupStat) get() reqState {
	s.Lock()
	defer s.Unlock()
	return s.reqState
}

// renew claims the slot if and only if it is not in use. On success it returns
// "" and the generation of the new claim, which the caller hands back to
// resetIfOwned to release it. On failure it returns the id already holding the
// slot and a generation of 0, which no claim ever has.
func (s *backupStat) renew(id string, path string, overrideBucket, overridePath string) (string, uint64) {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != "" {
		return s.reqState.ID, 0
	}
	s.generation++
	s.reqState.ID = id
	s.reqState.Path = path
	s.reqState.OverrideBucket = overrideBucket
	s.reqState.OverridePath = overridePath
	s.reqState.Starttime = time.Now().UTC()
	s.reqState.Status = backup.Started
	s.reqState.Err = ""
	if s.rememberedFailureID == id {
		// A retry under the same id: the earlier failure is no longer the
		// answer to a poll for it.
		s.rememberedFailureID, s.rememberedFailureReason = "", ""
	}
	return "", s.generation
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

// resetIfCancelled gives back the slot only when id owns it and that operation
// was cancelled. An operation still running under the same id is writing files,
// so its slot must survive. Checks and clears under one lock so a renew cannot
// slip in between and lose its claim. Reports whether the slot was cleared.
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

// setFailed ends the operation as failed together with the reason. Failed with
// no reason is worse than useless to a poller: the coordinator latches whatever
// a participant reports and stops asking, so an empty reason becomes the
// permanent answer for a failure that does have one. A reason-less failure is
// therefore substituted here rather than at the call sites, so that every
// caller gets the guarantee, including the ones passing on a reason a
// participant gave them.
func (s *backupStat) setFailed(reason string) {
	s.Lock()
	defer s.Unlock()
	s.setFailedLocked(reason)
}

// setFailedLocked must be called with the lock held.
func (s *backupStat) setFailedLocked(reason string) {
	if s.reqState.Status == backup.Cancelled {
		return
	}
	if reason == "" {
		reason = failureWithoutReason
	}
	s.reqState.Status = backup.Failed
	s.reqState.Err = reason
	s.rememberedFailureID = s.reqState.ID
	s.rememberedFailureReason = reason
}

// rememberedFailure reports why the operation with this id ended failed, for
// polls arriving after the slot was released. Absent for anything that did not
// end failed with a reason. The id has to match: a poll for one backup must
// never be answered with what happened to another.
func (s *backupStat) rememberedFailure(id string) (string, bool) {
	s.Lock()
	defer s.Unlock()
	if id == "" || s.rememberedFailureID != id {
		return "", false
	}
	return s.rememberedFailureReason, true
}

// resetIfOwned clears the slot only if generation is still the claim holding
// it. An operation whose slot was already handed to a newer one must not free
// the newcomer's claim: the slot is what keeps two operations from running on
// this node at once, so a false idle lets a second one claim it alongside the
// live one. Matching on the backup id would not be enough — the newer claim can
// carry the same id, since retrying a cancelled operation is a normal flow.
// Check-and-clear happens under one lock so a concurrent renew can't be lost.
// Reports whether it cleared.
func (s *backupStat) resetIfOwned(generation uint64) bool {
	s.Lock()
	defer s.Unlock()
	if s.generation != generation || s.reqState.ID == "" {
		return false
	}
	s.clear()
	return true
}

func (s *backupStat) set(st backup.Status) {
	s.Lock()
	defer s.Unlock()
	s.setLocked(st)
}

// setLocked must be called with the lock held.
func (s *backupStat) setLocked(st backup.Status) {
	// Cancelled is terminal - don't allow overwriting
	if s.reqState.Status == backup.Cancelled {
		return
	}
	s.reqState.Status = st
	// Every status other than Failed is reached through here, and none of them
	// has a reason. Keeping an earlier one would serve it next to a status it
	// does not belong to. The remembered failure is unaffected: it is what a
	// poll arriving after the slot is gone reads.
	s.reqState.Err = ""
}

// publishIfOwned mirrors an operation's own outcome onto the slot, but only
// while generation is still the claim holding it. Written this late, a newer
// operation may already have claimed the slot; writing unconditionally would
// stamp the finished operation's outcome onto the newcomer. Keyed on
// generation rather than id, matching [backupStat.resetIfOwned], since a
// retried operation can reuse the same id under a new claim. Reports whether
// it wrote.
func (s *backupStat) publishIfOwned(generation uint64, st backup.Status, reason string) bool {
	s.Lock()
	defer s.Unlock()
	if s.generation != generation || s.reqState.ID == "" {
		return false
	}
	if st == backup.Failed {
		s.setFailedLocked(reason)
		return true
	}
	s.setLocked(st)
	return true
}

// setIfOwned writes the status only if id still holds the slot, the write half
// of the same ownership rule as [backupStat.resetIfOwned]. A caller that does
// not derive id from the slot itself — a cancel, which takes it from object
// storage — would otherwise stamp whichever operation happens to hold it, and a
// slot reading Cancelled makes coordinator.commit abort the operation as
// "cancelled externally". Reports whether it wrote.
//
// Matches on the id, not on the generation resetIfOwned takes: the caller is
// not the holder and has no claim of its own, and cancelling whichever
// operation currently runs under that id is what a cancel is asking for. The
// same is true of [backupStat.resetIfCancelled], whose caller is a fresh
// restore attempt that has not claimed anything yet — which is why that one
// checks the status instead.
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
