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

	// generation counts the claims handed out; see [slotOwner].
	generation uint64

	// log is optional: without it, refused writes are dropped silently.
	log logrus.FieldLogger

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

// renew returns "" on success, or the id that already holds the slot.
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

// resetIfCancelled frees the slot only for a cancel that finished: Cancelling
// is still unwinding. Checks and clears under one lock, so no renew slips in.
func (s *backupStat) resetIfCancelled(id string) (bool, reqState) {
	s.Lock()
	defer s.Unlock()
	held := s.reqState
	if held.ID != id || held.Status != backup.Cancelled {
		return false, held
	}
	s.clear()
	return true, held
}

// canAdvanceTo reports whether next may overwrite the slot's status. Must be
// called with the lock held. Cancelled is final and a cancel in flight admits
// only Cancelled, so the slot never reports a cancelled operation as running.
// Finalizing refuses cancellations because a RAFT schema apply cannot be undone.
// Success and Failed stay open: a cancel may still land before the slot clears.
func (s *backupStat) canAdvanceTo(next backup.Status) bool {
	switch s.reqState.Status {
	case backup.Cancelled:
		return false
	case backup.Cancelling:
		return next == backup.Cancelled
	case backup.Finalizing:
		return !next.IsCancellation()
	default:
		return true
	}
}

// setStatus must be called with the lock held. It clears any earlier reason,
// so a cancel on a just-failed slot cannot answer polls with a stale error.
func (s *backupStat) setStatus(st backup.Status) {
	s.reqState.Status = st
	s.reqState.Err = ""
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

// The slot holds the status of the operation a node is running; renew hands
// it out to one operation at a time. A claim is what renew returns, and the
// only route by which that operation's writes reach the slot.
//
// Every write rechecks that the claim still holds the slot, so an operation
// that outlives its claim (a cancel frees the slot while its goroutine is
// still running) cannot overwrite the next operation's status. A claim is
// identified by a generation counter, not the backup id, which a retry reuses.
type slotOwner struct {
	stat       *backupStat
	generation uint64
}

// owns must be called with the lock held.
func (o slotOwner) owns() bool {
	return o.stat != nil && o.stat.generation == o.generation && o.stat.reqState.ID != ""
}

// droppedWrite carries a refused write, to be logged outside the slot's lock.
type droppedWrite struct {
	log    logrus.FieldLogger
	msg    string
	fields logrus.Fields
}

// emit must run with the slot's lock released: logrus writes synchronously,
// and every status poll reads the slot behind that lock.
func (d *droppedWrite) emit() {
	if d == nil {
		return
	}
	d.log.WithFields(d.fields).Info(d.msg)
}

// newDroppedWrite exists so "status stopped updating" is diagnosable as a
// refused write rather than silence. Must be called with the lock held.
func (o slotOwner) newDroppedWrite(st backup.Status) *droppedWrite {
	if o.stat == nil || o.stat.log == nil {
		return nil
	}
	msg := "slot write dropped: this operation no longer holds the slot"
	switch {
	case !o.owns():
		// keeps the message above
	case o.stat.reqState.Status == backup.Finalizing:
		msg = "slot write dropped: the restore is applying its schema and can no longer be cancelled"
	case o.stat.reqState.Status == backup.Cancelling:
		msg = "slot write dropped: a cancellation is in flight, and only its completion may follow"
	default:
		msg = "slot write dropped: the slot already reads a cancellation, which is its last word"
	}
	return &droppedWrite{
		log: o.stat.log,
		msg: msg,
		fields: logrus.Fields{
			"action":         "backup_slot_write",
			"dropped_status": st,
			"claim":          o.generation,
			"slot_claim":     o.stat.generation,
			"slot_holder":    o.stat.reqState.ID,
			"slot_status":    o.stat.reqState.Status,
		},
	}
}

func (o slotOwner) set(st backup.Status) bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	if !o.owns() || !o.stat.canAdvanceTo(st) {
		dropped := o.newDroppedWrite(st)
		o.stat.Unlock()
		dropped.emit()
		return false
	}
	o.stat.setStatus(st)
	o.stat.Unlock()
	return true
}

func (o slotOwner) setFailed(reason string) bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	if !o.owns() || !o.stat.canAdvanceTo(backup.Failed) {
		dropped := o.newDroppedWrite(backup.Failed)
		o.stat.Unlock()
		dropped.emit()
		return false
	}
	o.stat.setFailed(reason)
	o.stat.Unlock()
	return true
}

func (o slotOwner) holds() bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	return o.owns()
}

// status returns false once this claim no longer holds the slot. That means
// "nothing to learn here", not "not cancelled".
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

// release gives the slot back only while this claim still holds it: a stale
// claim must not free the slot the next operation now owns.
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

// claimOf is for the caller that has no claim of its own: a cancel reads the
// id out of object storage. Going through a claim lets its later writes tell
// that restore from a retry that took the slot under the same id since.
func (s *backupStat) claimOf(id string) slotOwner {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != id {
		return slotOwner{stat: s}
	}
	return slotOwner{stat: s, generation: s.generation}
}

// state reads the status and whether this claim still holds the slot under one
// lock, so a caller deciding on both cannot pair them across two moments.
func (o slotOwner) state() (bool, reqState) {
	if o.stat == nil {
		return false, reqState{}
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	return o.owns(), o.stat.reqState
}

// stamp is set, plus the slot state it found, for a caller that logs a miss.
func (o slotOwner) stamp(st backup.Status) (bool, reqState) {
	if o.stat == nil {
		return false, reqState{}
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	held := o.stat.reqState
	if !o.owns() || !o.stat.canAdvanceTo(st) {
		return false, held
	}
	o.stat.setStatus(st)
	return true, held
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

	// lastAsyncError is written without a lock or ownership check, so an earlier
	// operation can overwrite a later one's. Nothing in production reads it.
	lastAsyncError error
}

// setSlotLogger is for constructors only: it writes a field the slot's own lock
// guards everywhere else, so calling it later is a data race.
func (c *shardSyncChan) setSlotLogger(log logrus.FieldLogger) {
	c.lastOp.log = log
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
