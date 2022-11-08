package cluster

import (
	"context"
	"fmt"
)

func (c *TxManager) BeginReadTransaction(ctx context.Context,
	trType TransactionType,
) (*Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *TxManager) RespondReadTransaction(ctx context.Context,
	tx *Transaction, payload interface{},
) error {
	return fmt.Errorf("not implemented")
}

func (c *TxManager) CloseReadTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return fmt.Errorf("not implemented")
}
