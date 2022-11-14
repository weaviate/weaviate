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
	currentTransaction *Transaction
	remote             Remote
	commitFn           CommitFn
	responseFn         ResponseFn
	logger             logrus.FieldLogger
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
		c.currentTransaction = nil
		c.Unlock()

		return nil, errors.Wrap(err, "broadcast open transaction")
	}

	return c.currentTransaction, nil
}

func (c *TxManager) CommitWriteTransaction(ctx context.Context,
	tx *Transaction,
) error {
	c.Lock()
	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		c.Unlock()
		return ErrInvalidTransaction
	}

	c.Unlock()

	// now that we know we are dealing with a valid transaction: no  matter the
	// outcome, after this call, we should not have a local transaction anymore
	defer func() {
		c.Lock()
		c.currentTransaction = nil
		c.Unlock()
	}()

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
