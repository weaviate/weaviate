package cluster

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func (c *TxManager) BeginReadTransaction(ctx context.Context,
	trType TransactionType,
) (*Transaction, error) {
	// TODO: Is this identical with Write transaction? can we unify it?
	c.Lock()

	if c.currentTransaction != nil {
		c.Unlock()
		return nil, ErrConcurrentTransaction
	}

	c.currentTransaction = &Transaction{
		Type:    trType,
		ID:      uuid.New().String(),
		Payload: nil,
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

func (c *TxManager) CloseReadTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return fmt.Errorf("not implemented")
}
