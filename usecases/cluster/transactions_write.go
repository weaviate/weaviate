//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type TransactionType string

var (
	ErrConcurrentTransaction = errors.New("concurrent transaction")
	ErrInvalidTransaction    = errors.New("invalid transaction")
	ErrExpiredTransaction    = errors.New("transaction TTL expired")
)

type Remote interface {
	BroadcastTransaction(ctx context.Context, tx *Transaction) error
	BroadcastAbortTransaction(ctx context.Context, tx *Transaction) error
	BroadcastCommitTransaction(ctx context.Context, tx *Transaction) error
}

type (
	CommitFn   func(ctx context.Context, tx *Transaction) error
	ResponseFn func(ctx context.Context, tx *Transaction) error
)

type TxManager struct {
	sync.Mutex
	logger logrus.FieldLogger

	currentTransaction        *Transaction
	currentTransactionContext context.Context
	clearTransaction          func()

	remote     Remote
	commitFn   CommitFn
	responseFn ResponseFn

	// keep the ids of expired transactions around. This way, we can return a
	// nicer error message to the user. Instead of just an "invalid transaction"
	// which no longer exists, they will get an explicit error message mentioning
	// the timeout.
	expiredTxIDs []string
}

func newDummyCommitResponseFn() func(ctx context.Context, tx *Transaction) error {
	return func(ctx context.Context, tx *Transaction) error {
		return nil
	}
}

func NewTxManager(remote Remote, logger logrus.FieldLogger) *TxManager {
	return &TxManager{
		remote: remote,

		// by setting dummy fns that do nothing on default it is possible to run
		// the tx manager with only one set of functions. For example, if the
		// specific Tx is only ever used for broadcasting writes, there is no need
		// to set a responseFn. However, if the fn was nil, we'd panic. Thus a
		// dummy function is a resonable default - and much cleaner than a
		// nil-check on every call.
		commitFn:   newDummyCommitResponseFn(),
		responseFn: newDummyCommitResponseFn(),
	}
}

func (c *TxManager) resetTxExpiry(ctx context.Context, id string) {
	c.currentTransactionContext = ctx

	// to prevent a goroutine leak for the new routine we're spawning here,
	// register a way to terminate it in case the explicit cancel is called
	// before the context's done channel fires.
	clearCancelListener := make(chan struct{}, 1)

	c.clearTransaction = func() {
		c.currentTransaction = nil
		c.currentTransactionContext = nil
		c.clearTransaction = func() {}

		clearCancelListener <- struct{}{}
		close(clearCancelListener)
	}

	go func(id string) {
		ctxDone := ctx.Done()
		select {
		case <-clearCancelListener:
			return
		case <-ctxDone:
			c.Lock()
			defer c.Unlock()
			c.expiredTxIDs = append(c.expiredTxIDs, id)

			if c.currentTransaction == nil {
				// tx is already cleaned up, for example from a successful commit. Nothing to do for us
				return
			}

			if c.currentTransaction.ID != id {
				// tx was already cleaned up, then a new tx was started. Any action from
				// us would be destructive, as we'd accidently destroy a perfectly valid
				// tx
				return
			}

			c.clearTransaction()
		}
	}(id)
}

// expired is a helper to return a more meaningful error message to the user.
// Instead of just telling the user that an ID does not exist, this tracks that
// it once existed, but has been cleared because it expired.
//
// This method is not thread-safe as the assumption is that it is called from a
// thread-safe environment where a lock would already be held
func (c *TxManager) expired(id string) bool {
	for _, expired := range c.expiredTxIDs {
		if expired == id {
			return true
		}
	}

	return false
}

// SetCommitFn sets a function that is used in Write Transactions, you can
// read from the transaction payload and use that state to alter your local
// state
func (c *TxManager) SetCommitFn(fn CommitFn) {
	c.commitFn = fn
}

// SetResponseFn sets a function that is used in Read Transactions. The
// function sets the local state (by writing it into the Tx Payload). It can
// then be sent to other nodes. Consensus is not part of the ResponseFn. The
// coordinator - who initiated the Tx - is responsible for coming up with
// consensus. Deciding on Consensus requires insights into business logic, as
// from the TX's perspective payloads are opaque.
func (c *TxManager) SetResponseFn(fn ResponseFn) {
	c.responseFn = fn
}

// Begin a Transaction with the specified type and payload. By default
// transactions do not ever expire. However, they inherit the deadline from the
// passed in context, so to make a Transaction expire, pass in a context that
// itself expires.
func (c *TxManager) BeginTransaction(ctx context.Context, trType TransactionType,
	payload interface{},
) (*Transaction, error) {
	c.Lock()

	if c.currentTransaction != nil {
		c.Unlock()
		return nil, ErrConcurrentTransaction
	}

	c.currentTransaction = &Transaction{
		Type:    trType,
		ID:      uuid.New().String(),
		Payload: payload,
	}
	if dl, ok := ctx.Deadline(); ok {
		c.currentTransaction.Deadline = dl
	}
	c.Unlock()

	c.resetTxExpiry(ctx, c.currentTransaction.ID)

	if err := c.remote.BroadcastTransaction(ctx, c.currentTransaction); err != nil {
		// we could not open the transaction on every node, therefore we need to
		// abort it everywhere.

		if err := c.remote.BroadcastAbortTransaction(ctx, c.currentTransaction); err != nil {
			c.logger.WithFields(logrus.Fields{
				"action": "broadcast_abort_transaction",
				"id":     c.currentTransaction.ID,
			}).WithError(err).Errorf("broadcast tx abort failed")
		}

		c.Lock()
		c.clearTransaction()
		c.Unlock()

		return nil, errors.Wrap(err, "broadcast open transaction")
	}

	c.Lock()
	defer c.Unlock()
	return c.currentTransaction, nil
}

func (c *TxManager) CommitWriteTransaction(ctx context.Context,
	tx *Transaction,
) error {
	c.Lock()
	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		expired := c.expired(tx.ID)
		c.Unlock()
		if expired {
			return ErrExpiredTransaction
		}
		return ErrInvalidTransaction
	}

	c.Unlock()

	// now that we know we are dealing with a valid transaction: no  matter the
	// outcome, after this call, we should not have a local transaction anymore
	defer c.clearTransaction()

	if err := c.remote.BroadcastCommitTransaction(ctx, tx); err != nil {
		// we could not open the transaction on every node, therefore we need to
		// abort it everywhere.

		if err := c.remote.BroadcastAbortTransaction(ctx, tx); err != nil {
			c.logger.WithFields(logrus.Fields{
				"action": "broadcast_abort_transaction",
				"id":     tx.ID,
			}).WithError(err).Errorf("broadcast tx abort failed")
		}

		return errors.Wrap(err, "broadcast commit transaction")
	}

	return nil
}

func (c *TxManager) IncomingBeginTransaction(ctx context.Context,
	tx *Transaction,
) error {
	c.Lock()
	defer c.Unlock()

	if c.currentTransaction != nil && c.currentTransaction.ID != tx.ID {
		return ErrConcurrentTransaction
	}

	c.currentTransaction = tx
	c.responseFn(ctx, tx)

	return nil
}

func (c *TxManager) IncomingAbortTransaction(ctx context.Context,
	tx *Transaction,
) {
	c.Lock()
	defer c.Unlock()

	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		// don't do anything
		return
	}

	c.currentTransaction = nil
}

func (c *TxManager) IncomingCommitTransaction(ctx context.Context,
	tx *Transaction,
) error {
	c.Lock()
	defer c.Unlock()

	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		return ErrInvalidTransaction
	}

	// use transaction from cache, not passed in for two reason: a.) protect
	// against the transaction being manipulated after being created, b.) allow
	// an "empty" transaction that only contains the id for less network overhead
	// (we don't need to pass the payload around anymore, after it's successfully
	// opened - every node has a copy of the payload now)
	err := c.commitFn(ctx, c.currentTransaction)
	if err != nil {
		return err
	}

	// TODO: only clean up on success - does this make sense?
	c.currentTransaction = nil
	return nil
}

type Transaction struct {
	ID       string
	Type     TransactionType
	Payload  interface{}
	Deadline time.Time
}

func ContextFromTx(tx *Transaction) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if tx.Deadline.UnixMilli() == 0 {
		return ctx, func() {}
	}

	return context.WithDeadline(ctx, tx.Deadline)
}

func inheritCtxDeadline(parent,
	otherParentWithDeadline context.Context,
) (context.Context, context.CancelFunc) {
	if dl, ok := otherParentWithDeadline.Deadline(); ok {
		return context.WithDeadline(parent, dl)
	}

	return parent, func() {}
}
