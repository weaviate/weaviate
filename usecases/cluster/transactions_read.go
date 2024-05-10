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
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (c *TxManager) CloseReadTransaction(ctx context.Context,
	tx *Transaction,
) error {
	c.Lock()
	if c.currentTransaction == nil || c.currentTransaction.ID != tx.ID {
		c.Unlock()
		return ErrInvalidTransaction
	}

	c.Unlock()
	c.slowLog.Update("close_read_started")

	// now that we know we are dealing with a valid transaction: no  matter the
	// outcome, after this call, we should not have a local transaction anymore
	defer func() {
		c.Lock()
		c.currentTransaction = nil
		monitoring.GetMetrics().SchemaTxClosed.With(prometheus.Labels{
			"ownership": "coordinator",
			"status":    "close_read",
		}).Inc()
		took := time.Since(c.currentTransactionBegin)
		monitoring.GetMetrics().SchemaTxDuration.With(prometheus.Labels{
			"ownership": "coordinator",
			"status":    "close_read",
		}).Observe(took.Seconds())
		c.slowLog.Close("closed_read")
		c.Unlock()
	}()

	if err := c.remote.BroadcastCommitTransaction(ctx, tx); err != nil {
		// we could not open the transaction on every node, therefore we need to
		// abort it everywhere.

		if err := c.remote.BroadcastAbortTransaction(ctx, tx); err != nil {
			c.logger.WithFields(logrus.Fields{
				"action": "broadcast_abort_read_transaction",
				"id":     tx.ID,
			}).WithError(err).Error("broadcast tx (read-only) abort failed")
		}

		return errors.Wrap(err, "broadcast commit read transaction")
	}

	return nil
}
