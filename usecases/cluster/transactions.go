package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type TransactionType string

var (
	ErrConcurrentTransaction = errors.New("concurrent transaction")
	ErrInvalidTransaction    = errors.New("invalid transaction")
)

const (
	AddClass TransactionType = "add_class"
)

type Remote interface {
	BroadcastTransaction(ctx context.Context, tx *Transaction) error
	BroadcastAbortTransaction(ctx context.Context, tx *Transaction) error
	BroadcastCommitTransaction(ctx context.Context, tx *Transaction) error
}

type CommitFn func(ctx context.Context, tx *Transaction) error

type TxManager struct {
	sync.Mutex
	currentTransaction *Transaction
	remote             Remote
	commitFn           CommitFn
}

func NewTxManager(remote Remote) *TxManager {
	return &TxManager{remote: remote}
}

func (c *TxManager) SetCommitFn(fn CommitFn) {
	c.commitFn = fn
}

func (c *TxManager) BeginTransaction(ctx context.Context, trType TransactionType,
	payload interface{}) (*Transaction, error) {
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
	c.Unlock()

	if err := c.remote.BroadcastTransaction(ctx, c.currentTransaction); err != nil {
		// we could not open the transaction on every node, therefore we need to
		// abort it everywhere.

		if err := c.remote.BroadcastAbortTransaction(ctx, c.currentTransaction); err != nil {
			// TODO WARN with structured logging
			fmt.Println(err)
		}

		c.Lock()
		c.currentTransaction = nil
		c.Unlock()

		return nil, errors.Wrap(err, "broadcast open transaction")
	}

	return c.currentTransaction, nil
}

func (c *TxManager) CommitTransaction(ctx context.Context, tx *Transaction) error {
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
			// TODO WARN with structured logging
			fmt.Println(err)
		}

		return errors.Wrap(err, "broadcast commit transaction")
	}

	return nil
}

func (c *TxManager) IncomingBeginTransaction(ctx context.Context,
	tx *Transaction) error {
	c.Lock()
	defer c.Unlock()

	if c.currentTransaction != nil && c.currentTransaction.ID != tx.ID {
		return ErrConcurrentTransaction
	}

	c.currentTransaction = tx
	return nil
}

func (c *TxManager) IncomingAbortTransaction(ctx context.Context,
	tx *Transaction) {
	c.Lock()
	defer c.Unlock()

	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		// don't do anything
		return
	}

	c.currentTransaction = nil
}

func (c *TxManager) IncomingCommitTransaction(ctx context.Context,
	tx *Transaction) error {
	c.Lock()
	defer c.Unlock()

	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		return ErrInvalidTransaction
	}

	return c.commitFn(ctx, tx)
}

type Transaction struct {
	ID      string
	Type    TransactionType
	Payload interface{}
}

type AddClassPayload struct {
	Class *models.Class
	State *sharding.State
}
