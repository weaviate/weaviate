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

	// generation counts the claims handed out; see [slotOwner] for why identity
	// is the generation and not the (reusable) backup id.
	generation uint64

	// log records the status writes the slot refuses, i.e. the ones made
	// through [slotOwner.set] and [slotOwner.setFailed]. Optional: a zero
	// backupStat is usable, it just says nothing.
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

// renew claims the slot if it is not in use, returning "" and the claim used
// to write to it and release it. On failure it returns the id already
// holding the slot and a claim that owns nothing.
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

// resetIfCancelled gives back the slot only when id owns it and it has fully
// cancelled (Cancelling, still unwinding, leaves it alone). Checks and clears
// under one lock so a renew cannot slip in between. Reports whether it
// cleared, and the state it found, read together to avoid pairing them
// against different moments.
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

// canAdvanceTo reports whether next may overwrite the status the slot holds.
// Must be called with the lock held.
//
// Cancellation is the operation's last word: Cancelled is final, and a cancel
// in flight may only advance to Cancelled — anything else would walk a
// requested cancel back to "running". Finalizing is the mirror image: schema
// apply over RAFT can no longer be stopped, so a cancellation there is
// refused and the restore reports the outcome it actually had.
//
// Success and Failed are deliberately not terminal here: they're published
// just before the slot releases, so a cancel landing in that window may still
// stamp Cancelled over them. The descriptor carries the real outcome once the
// slot clears.
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

// setStatus publishes a status that carries no reason of its own. Must be
// called with the lock held. Clears any earlier reason, so a cancel landing
// on a just-failed slot doesn't answer a poll with CANCELLING plus a stale
// error.
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

// slotOwner is the claim [backupStat.renew] hands to the operation that took
// the slot, and the only way that operation reads or writes it. Every method
// checks the claim still holds the slot, so an operation that outlives it
// (its goroutine keeps running after a cancel frees the slot for the next
// claimant) cannot land a stale write on that next operation's state.
//
// Identity is the generation, not the backup id: a cancel-then-retry under
// the same id is normal, so the id alone can't tell two claims apart. The
// zero value owns nothing; every write through it is a no-op.
type slotOwner struct {
	stat       *backupStat
	generation uint64
}

// owns must be called with the lock held.
func (o slotOwner) owns() bool {
	return o.stat != nil && o.stat.generation == o.generation && o.stat.reqState.ID != ""
}

// droppedWrite carries a refused write so it can be logged outside the
// slot's lock.
type droppedWrite struct {
	log    logrus.FieldLogger
	msg    string
	fields logrus.Fields
}

// emit must run with the slot's lock released: logrus writes synchronously,
// and every status poll reads the slot behind that lock.
//
// Info, not Debug: the default log level is Info, and a dropped write is
// often what a support case starts from.
func (d *droppedWrite) emit() {
	if d == nil {
		return
	}
	d.log.WithFields(d.fields).Info(d.msg)
}

// droppedWrite describes a write the slot refused, so "status stopped
// updating" is diagnosable as a refusal rather than silence. Returns nil when
// the slot has no logger. Must be called with the lock held.
func (o slotOwner) droppedWrite(st backup.Status) *droppedWrite {
	if o.stat == nil || o.stat.log == nil {
		return nil
	}
	msg := "slot write dropped: this operation no longer holds the slot"
	switch {
	case !o.owns():
		// keeps the message above
	case o.stat.reqState.Status == backup.Finalizing:
		msg = "slot write dropped: the restore is applying its schema and can no longer be cancelled"
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

// set publishes a status on the slot. Reports whether it wrote.
func (o slotOwner) set(st backup.Status) bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	if !o.owns() || !o.stat.canAdvanceTo(st) {
		dropped := o.droppedWrite(st)
		o.stat.Unlock()
		dropped.emit()
		return false
	}
	o.stat.setStatus(st)
	o.stat.Unlock()
	return true
}

// setFailed ends the operation as failed together with the reason it ended for.
// Reports whether it wrote.
func (o slotOwner) setFailed(reason string) bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	if !o.owns() || !o.stat.canAdvanceTo(backup.Failed) {
		dropped := o.droppedWrite(backup.Failed)
		o.stat.Unlock()
		dropped.emit()
		return false
	}
	o.stat.setFailed(reason)
	o.stat.Unlock()
	return true
}

// holds reports whether this claim still owns the slot, i.e. whether the slot
// still belongs to this operation.
func (o slotOwner) holds() bool {
	if o.stat == nil {
		return false
	}
	o.stat.Lock()
	defer o.stat.Unlock()
	return o.owns()
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

// release gives the slot back, only while this claim still holds it: an
// outlived claim must not free the slot the next operation now owns. Reports
// whether it cleared.
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

// setIfOwned writes the status only if id still holds the slot. Unlike the
// [slotOwner] methods, the caller has no claim of its own (a cancel gets id
// from object storage) and matches on id instead, to avoid stamping whichever
// operation happens to hold the slot. Reports whether it wrote, and the state
// found, read together so a caller logging both can't pair them across time.
func (s *backupStat) setIfOwned(id string, st backup.Status) (bool, reqState) {
	s.Lock()
	defer s.Unlock()
	held := s.reqState
	if held.ID != id || !s.canAdvanceTo(st) {
		return false, held
	}
	s.setStatus(st)
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

	// lastAsyncError used for debugging when no metadata is created
	lastAsyncError error
}

// setSlotLogger wires the operation slot to a logger, so the writes it refuses
// leave something behind. Constructors only: it writes a field that the slot's
// own lock guards everywhere else, so wiring a logger once the slot is reachable
// from another goroutine is a data race.
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
