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
// "" and the claim, which is how the claiming operation writes to the slot and
// how it gives it back. On failure it returns the id already holding the slot
// and a claim that owns nothing.
func (s *backupStat) renew(id string, path string, overrideBucket, overridePath string) (string, slotOwner) {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != "" {
		return s.reqState.ID, slotOwner{}
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
	return "", slotOwner{stat: s, generation: s.generation}
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
// has been cancelled. A cancel that is still in flight (Cancelling) leaves the
// slot alone: the operation is only cancelled once its own goroutine says so,
// and until then it is still writing files. Checks and clears under one lock so
// a renew cannot slip in between and lose its claim. Reports whether the slot
// was cleared.
//
// The goroutine of the cancelled operation can still be running when this
// returns, and will keep writing to the slot for as long as it takes to unwind.
// Those writes carry the claim it was given (see [slotOwner]), so they no-op
// once the slot is free or claimed by somebody else.
func (s *backupStat) resetIfCancelled(id string) bool {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != id || s.reqState.Status != backup.Cancelled {
		return false
	}
	s.clear()
	return true
}

// canAdvanceTo reports whether next may overwrite the status the slot holds.
// Must be called with the lock held.
//
// A cancellation is the operation's last word: Cancelled is final, and a cancel
// in flight may only go on to Cancelled. Any other status walks a cancel the
// operator already asked for back to a running one, which is what a poll then
// reports.
func (s *backupStat) canAdvanceTo(next backup.Status) bool {
	switch s.reqState.Status {
	case backup.Cancelled:
		return false
	case backup.Cancelling:
		return next == backup.Cancelled
	default:
		return true
	}
}

// setFailed ends the operation as failed together with the reason. Must be
// called with the lock held.
//
// Failed with no reason is worse than useless to a poller: the coordinator
// latches whatever a participant reports and stops asking, so an empty reason
// becomes the permanent answer for a failure that does have one. A reason-less
// failure is therefore substituted here rather than at the call sites, so that
// every caller gets the guarantee, including the ones passing on a reason a
// participant gave them.
func (s *backupStat) setFailed(reason string) {
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

// slotOwner is the claim [backupStat.renew] hands to the operation that took
// the slot, and the only way that operation reads or writes it. Every method
// checks that this claim is still the one holding the slot, so an operation
// that outlives its claim cannot touch the slot of the one that came after it.
//
// That outliving is a normal flow, not an edge case: a cancel frees the slot
// while the cancelled operation is still unwinding, and the next restore claims
// it immediately. Without the check the old goroutine's last status write lands
// on the new claim, and a restore that just started is reported as SUCCESS —
// or, worse, as CANCELLED, which makes [coordinator.commit] abort it.
//
// Ownership is the generation, not the backup id: ids are user-supplied and a
// cancel-then-retry under the same id is a normal flow, so the id alone cannot
// tell two claims apart. The zero value owns nothing and every write through it
// is a no-op.
type slotOwner struct {
	stat       *backupStat
	generation uint64
}

// owns must be called with the lock held.
func (o slotOwner) owns() bool {
	return o.stat != nil && o.generation != 0 &&
		o.stat.generation == o.generation && o.stat.reqState.ID != ""
}

// set publishes a status on the slot. Reports whether it wrote.
func (o slotOwner) set(st backup.Status) bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	if !o.owns() || !o.stat.canAdvanceTo(st) {
		return false
	}
	o.stat.reqState.Status = st
	// Every status other than Failed is reached through here, and none of them
	// has a reason. Keeping an earlier one would serve it next to a status it
	// does not belong to. The remembered failure is unaffected: it is what a
	// poll arriving after the slot is gone reads.
	o.stat.reqState.Err = ""
	return true
}

// setFailed ends the operation as failed together with the reason it ended for.
// Reports whether it wrote.
func (o slotOwner) setFailed(reason string) bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	if !o.owns() || !o.stat.canAdvanceTo(backup.Failed) {
		return false
	}
	o.stat.setFailed(reason)
	return true
}

// status is the slot's status as long as this claim still holds it. The second
// return is false once it does not, which readers take as "nothing to learn
// here" rather than as the status of an operation that is not theirs.
func (o slotOwner) status() (backup.Status, bool) {
	if o.stat == nil {
		return "", false
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	if !o.owns() {
		return "", false
	}
	return o.stat.reqState.Status, true
}

// release gives the slot back, and only while this claim still holds it. An
// operation whose slot was already handed to a newer one must not free the
// newcomer's claim: the slot is what keeps two operations from running on this
// node at once, so a false idle lets a second one claim it alongside the live
// one. Reports whether it cleared.
func (o slotOwner) release() bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	if !o.owns() {
		return false
	}
	o.stat.clear()
	return true
}

// setIfOwned writes the status only if id still holds the slot. It is the one
// write that does not come from the holder: a cancel takes the id from object
// storage, and without the check it would stamp whichever operation happens to
// hold the slot — a slot reading Cancelled makes coordinator.commit abort the
// operation as "cancelled externally". Reports whether it wrote.
//
// Matches on the id, not on the generation a [slotOwner] carries: the caller is
// not the holder and has no claim of its own, and cancelling whichever
// operation currently runs under that id is what a cancel is asking for. The
// same is true of [backupStat.resetIfCancelled], whose caller is a fresh
// restore attempt that has not claimed anything yet — which is why that one
// checks the status instead.
func (s *backupStat) setIfOwned(id string, st backup.Status) bool {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != id || !s.canAdvanceTo(st) {
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
