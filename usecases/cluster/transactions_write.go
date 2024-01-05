//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

type TransactionType string

var (
	ErrConcurrentTransaction = errors.New("concurrent transaction")
	ErrInvalidTransaction    = errors.New("invalid transaction")
	ErrExpiredTransaction    = errors.New("transaction TTL expired")
	ErrNotReady              = errors.New("server is not ready: either starting up or shutting down")
)

type Remote interface {
	BroadcastTransaction(ctx context.Context, tx *Transaction) error
	BroadcastAbortTransaction(ctx context.Context, tx *Transaction) error
	BroadcastCommitTransaction(ctx context.Context, tx *Transaction) error
}

type (
	CommitFn   func(ctx context.Context, tx *Transaction) error
	ResponseFn func(ctx context.Context, tx *Transaction) ([]byte, error)
)

type TxManager struct {
	sync.Mutex
	logger logrus.FieldLogger

	currentTransaction        *Transaction
	currentTransactionContext context.Context
	clearTransaction          func()

	// any time we start working on a commit, we need to add to this WaitGroup.
	// It will block shutdwon until the commit has completed to make sure that we
	// can't accidentally shutdown while a tx is committing.
	ongoingCommits sync.WaitGroup

	// when a shutdown signal has been received, we will no longer accept any new
	// tx's or commits
	acceptIncoming bool

	// read transactions that need to run at start can still be served, they have
	// no side-effects on the node that accepts them.
	//
	// If we disallowed them completely, then two unready nodes would be in a
	// deadlock as they each require information from the other(s) who can't
	// answerbecause they're not ready.
	allowUnready []TransactionType

	remote     Remote
	commitFn   CommitFn
	responseFn ResponseFn

	// keep the ids of expired transactions around. This way, we can return a
	// nicer error message to the user. Instead of just an "invalid transaction"
	// which no longer exists, they will get an explicit error message mentioning
	// the timeout.
	expiredTxIDs []string

	persistence Persistence
}

func newDummyCommitResponseFn() func(ctx context.Context, tx *Transaction) error {
	return func(ctx context.Context, tx *Transaction) error {
		return nil
	}
}

func newDummyResponseFn() func(ctx context.Context, tx *Transaction) ([]byte, error) {
	return func(ctx context.Context, tx *Transaction) ([]byte, error) {
		return nil, nil
	}
}

func NewTxManager(remote Remote, persistence Persistence,
	logger logrus.FieldLogger,
) *TxManager {
	return &TxManager{
		remote: remote,

		// by setting dummy fns that do nothing on default it is possible to run
		// the tx manager with only one set of functions. For example, if the
		// specific Tx is only ever used for broadcasting writes, there is no need
		// to set a responseFn. However, if the fn was nil, we'd panic. Thus a
		// dummy function is a reasonable default - and much cleaner than a
		// nil-check on every call.
		commitFn:    newDummyCommitResponseFn(),
		responseFn:  newDummyResponseFn(),
		logger:      logger,
		persistence: persistence,

		// ready to serve incoming requests
		acceptIncoming: false,
	}
}

func (c *TxManager) StartAcceptIncoming() {
	c.Lock()
	defer c.Unlock()

	c.acceptIncoming = true
}

func (c *TxManager) SetAllowUnready(types []TransactionType) {
	c.Lock()
	defer c.Unlock()

	c.allowUnready = types
}

// HaveDanglingTxs is a way to check if there are any uncommitted transactions
// in the durable storage. This can be used to make decisions about whether a
// failed schema check can be temporarily ignored - with the assumption that
// applying the dangling txs will fix the issue.
func (c *TxManager) HaveDanglingTxs(ctx context.Context,
	allowedTypes []TransactionType,
) (found bool) {
	c.persistence.IterateAll(context.Background(), func(tx *Transaction) {
		if !slices.Contains(allowedTypes, tx.Type) {
			return
		}
		found = true
	})

	return
}

// TryResumeDanglingTxs loops over the existing transactions and applies them.
// It only does so if the transaction type is explicitly listed as allowed.
// This is because - at the time of creating this - we were not sure if all
// transaction commit functions are idempotent. If one would not be, then
// reapplying a tx or tx commit could potentially be dangerous, as we don't
// know if it was already applied prior to the node death.
//
// For example, think of a "add property 'foo'" tx, that does nothing but
// append the property to the schema. If this ran twice, we might now end up
// with two duplicate properties with the name 'foo' which could in turn create
// other problems. To make sure all txs are resumable (which is what we want
// because that's the only way to avoid schema issues), we need to make sure
// that every single tx is idempotent, then add them to the allow list.
//
// One other limitation is that this method currently does nothing to check if
// a tx was really committed or not. In an ideal world, the node would contact
// the other nodes and ask. However, this sipmler implementation does not do
// this check. Instead [HaveDanglingTxs] is used in combination with the schema
// check. If the schema is not out of sync in the first place, no txs will be
// applied. This does not cover all edge cases, but it seems to work for now.
// This should be improved in the future.
func (c *TxManager) TryResumeDanglingTxs(ctx context.Context,
	allowedTypes []TransactionType,
) (applied bool, err error) {
	c.persistence.IterateAll(context.Background(), func(tx *Transaction) {
		if !slices.Contains(allowedTypes, tx.Type) {
			c.logger.WithField("action", "resume_transaction").
				WithField("transaction_id", tx.ID).
				WithField("transaction_type", tx.Type).
				Warnf("dangling transaction %q of type %q is not known to be resumable - skipping",
					tx.ID, tx.Type)

			return
		}
		if err = c.commitFn(ctx, tx); err != nil {
			return
		}

		applied = true
		c.logger.WithField("action", "resume_transaction").
			WithField("transaction_id", tx.ID).
			WithField("transaction_type", tx.Type).
			Infof("successfully resumed dangling transaction %q of type %q",
				tx.ID, tx.Type)
	})

	return
}

func (c *TxManager) resetTxExpiry(ttl time.Duration, id string) {
	cancel := func() {}
	ctx := context.Background()
	if ttl == 0 {
		c.currentTransactionContext = context.Background()
	} else {
		ctx, cancel = context.WithTimeout(ctx, ttl)
		c.currentTransactionContext = ctx
	}

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
			cancel()
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
				// us would be destructive, as we'd accidentally destroy a perfectly valid
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

// Begin a Transaction with the specified type and payload. Transactions expire
// after the specified TTL. For a transaction that does not ever expire, pass
// in a ttl of 0. When choosing TTLs keep in mind that clocks might be slightly
// skewed in the cluster, therefore set your TTL for desiredTTL +
// toleratedClockSkew
//
// Regular transactions cannot be opened if the cluster is not considered
// healthy.
func (c *TxManager) BeginTransaction(ctx context.Context, trType TransactionType,
	payload interface{}, ttl time.Duration,
) (*Transaction, error) {
	return c.beginTransaction(ctx, trType, payload, ttl, false)
}

// Begin a Transaction that does not require the whole cluster to be healthy.
// This can be used for example in bootstrapping situations when not all nodes
// are present yet, or in disaster recovery situations when a node needs to run
// a transaction in order to re-join a cluster.
func (c *TxManager) BeginTransactionTolerateNodeFailures(ctx context.Context, trType TransactionType,
	payload interface{}, ttl time.Duration,
) (*Transaction, error) {
	return c.beginTransaction(ctx, trType, payload, ttl, true)
}

func (c *TxManager) beginTransaction(ctx context.Context, trType TransactionType,
	payload interface{}, ttl time.Duration, tolerateNodeFailures bool,
) (*Transaction, error) {
	c.Lock()

	if c.currentTransaction != nil {
		c.Unlock()
		return nil, ErrConcurrentTransaction
	}

	tx := &Transaction{
		Type:                 trType,
		ID:                   uuid.New().String(),
		Payload:              payload,
		TolerateNodeFailures: tolerateNodeFailures,
	}
	if ttl > 0 {
		tx.Deadline = time.Now().Add(ttl)
	} else {
		// UnixTime == 0 represents unlimited
		tx.Deadline = time.UnixMilli(0)
	}
	c.currentTransaction = tx
	c.Unlock()

	c.resetTxExpiry(ttl, c.currentTransaction.ID)

	if err := c.remote.BroadcastTransaction(ctx, tx); err != nil {
		// we could not open the transaction on every node, therefore we need to
		// abort it everywhere.

		if err := c.remote.BroadcastAbortTransaction(ctx, tx); err != nil {
			c.logger.WithFields(logrus.Fields{
				"action": "broadcast_abort_transaction",
				// before https://github.com/weaviate/weaviate/issues/2625 the next
				// line would read
				//
				// "id": c.currentTransaction.ID
				//
				// which had the potential for races. The tx itself is immutable and
				// therefore always thread-safe. However, the association between the tx
				// manager and the current tx is mutable, therefore the
				// c.currentTransaction pointer could be nil (nil pointer panic) or
				// point to another tx (incorrect log).
				"id": tx.ID,
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

	if !c.acceptIncoming {
		c.Unlock()
		return ErrNotReady
	}

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
	defer func() {
		c.Lock()
		c.clearTransaction()
		c.Unlock()
	}()

	if err := c.remote.BroadcastCommitTransaction(ctx, tx); err != nil {
		// the broadcast failed, but we can't do anything about it. If we would
		// broadcast an "abort" now (as a previous version did) we'd likely run
		// into an inconsistency down the line. Network requests have variable
		// time, so there's a chance some nodes would see the abort before the
		// commit and vice-versa. Given enough nodes, we would end up with an
		// inconsistent state.
		//
		// A failed commit means the node that didn't receive the commit needs to
		// figure out itself how to get back to the correct state (e.g. by
		// recovering from a persisted tx), don't jeopardize all the other nodes as
		// a result!
		return errors.Wrap(err, "broadcast commit transaction")
	}

	return nil
}

func (c *TxManager) IncomingBeginTransaction(ctx context.Context,
	tx *Transaction,
) ([]byte, error) {
	c.Lock()
	defer c.Unlock()

	if !c.acceptIncoming && !slices.Contains(c.allowUnready, tx.Type) {
		return nil, ErrNotReady
	}

	if c.currentTransaction != nil && c.currentTransaction.ID != tx.ID {
		return nil, ErrConcurrentTransaction
	}

	if err := c.persistence.StoreTx(ctx, tx); err != nil {
		return nil, fmt.Errorf("make tx durable: %w", err)
	}

	c.currentTransaction = tx
	data, err := c.responseFn(ctx, tx)
	if err != nil {
		return nil, err
	}
	var ttl time.Duration
	if tx.Deadline.UnixMilli() != 0 {
		ttl = time.Until(tx.Deadline)
	}
	c.resetTxExpiry(ttl, tx.ID)

	return data, nil
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
	if err := c.persistence.DeleteTx(ctx, tx.ID); err != nil {
		c.logger.WithError(err).Errorf("abort tx: %s", err)
	}
}

func (c *TxManager) IncomingCommitTransaction(ctx context.Context,
	tx *Transaction,
) error {
	c.ongoingCommits.Add(1)
	defer c.ongoingCommits.Done()

	c.Lock()
	defer c.Unlock()

	if !c.acceptIncoming {
		return ErrNotReady
	}

	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		expired := c.expired(tx.ID)
		if expired {
			return ErrExpiredTransaction
		}
		return ErrInvalidTransaction
	}

	// use transaction from cache, not passed in for two reason: a. protect
	// against the transaction being manipulated after being created, b. allow
	// an "empty" transaction that only contains the id for less network overhead
	// (we don't need to pass the payload around anymore, after it's successfully
	// opened - every node has a copy of the payload now)
	err := c.commitFn(ctx, c.currentTransaction)
	if err != nil {
		return err
	}

	// TODO: only clean up on success - does this make sense?
	c.currentTransaction = nil

	if err := c.persistence.DeleteTx(ctx, tx.ID); err != nil {
		return fmt.Errorf("close tx on disk: %w", err)
	}

	return nil
}

func (c *TxManager) Shutdown() {
	c.Lock()
	c.acceptIncoming = false
	c.Unlock()

	c.ongoingCommits.Wait()
}

type Transaction struct {
	ID       string
	Type     TransactionType
	Payload  interface{}
	Deadline time.Time

	// If TolerateNodeFailures is false (the default) a transaction cannot be
	// opened or committed if a node is confirmed dead. If a node is only
	// suspected dead, the TxManager will try, but abort unless all nodes ACK.
	TolerateNodeFailures bool
}

type Persistence interface {
	StoreTx(ctx context.Context, tx *Transaction) error
	DeleteTx(ctx context.Context, txID string) error
	IterateAll(ctx context.Context, cb func(tx *Transaction)) error
}
