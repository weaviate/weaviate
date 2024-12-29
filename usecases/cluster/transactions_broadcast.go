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
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
)

type TxBroadcaster struct {
	state       MemberLister
	client      Client
	consensusFn ConsensusFn
	ideal       *IdealClusterState
	logger      logrus.FieldLogger
}

// The Broadcaster is the link between the the current node and all other nodes
// during a tx operation. This makes it a natural place to inject a consensus
// function for read transactions. How consensus is reached is completely opaque
// to the broadcaster and can be controlled through custom business logic.
type ConsensusFn func(ctx context.Context,
	in []*Transaction) (*Transaction, error)

type Client interface {
	OpenTransaction(ctx context.Context, host string, tx *Transaction) error
	AbortTransaction(ctx context.Context, host string, tx *Transaction) error
	CommitTransaction(ctx context.Context, host string, tx *Transaction) error
}

type MemberLister interface {
	AllNames() []string
	Hostnames() []string
}

func NewTxBroadcaster(state MemberLister, client Client, logger logrus.FieldLogger) *TxBroadcaster {
	ideal := NewIdealClusterState(state, logger)
	return &TxBroadcaster{
		state:  state,
		client: client,
		ideal:  ideal,
		logger: logger,
	}
}

func (t *TxBroadcaster) SetConsensusFunction(fn ConsensusFn) {
	t.consensusFn = fn
}

func (t *TxBroadcaster) BroadcastTransaction(rootCtx context.Context, tx *Transaction) error {
	if !tx.TolerateNodeFailures {
		if err := t.ideal.Validate(); err != nil {
			return fmt.Errorf("tx does not tolerate node failures: %w", err)
		}
	}

	hosts := t.state.Hostnames()
	resTx := make([]*Transaction, len(hosts))
	eg := enterrors.NewErrorGroupWrapper(t.logger)
	for i, host := range hosts {
		i := i       // https://golang.org/doc/faq#closures_and_goroutines
		host := host // https://golang.org/doc/faq#closures_and_goroutines

		eg.Go(func() error {
			// make sure we don't block forever if the caller passes in an unlimited
			// context. If another node does not respond within the timeout, consider
			// the tx open attempt failed.
			ctx, cancel := context.WithTimeout(rootCtx, 30*time.Second)
			defer cancel()

			t.logger.WithFields(logrus.Fields{
				"action":   "broadcast_transaction",
				"duration": 30 * time.Second,
			}).Debug("context.WithTimeout")

			// the client call can mutate the tx, so we need to work with copies to
			// prevent a race and to be able to keep all individual results, so they
			// can be passed to the consensus fn
			resTx[i] = copyTx(tx)
			if err := t.client.OpenTransaction(ctx, host, resTx[i]); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		}, host)
	}

	err := eg.Wait()
	if err != nil {
		return err
	}

	if t.consensusFn != nil {
		merged, err := t.consensusFn(rootCtx, resTx)
		if err != nil {
			return fmt.Errorf("try to reach consenus: %w", err)
		}

		if merged != nil {
			tx.Payload = merged.Payload
		}
	}

	return nil
}

func (t *TxBroadcaster) BroadcastAbortTransaction(rootCtx context.Context, tx *Transaction) error {
	eg := enterrors.NewErrorGroupWrapper(t.logger)
	for _, host := range t.state.Hostnames() {
		host := host // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			// make sure we don't block forever if the caller passes in an unlimited
			// context. If another node does not respond within the timeout, consider
			// the tx abort attempt failed.
			ctx, cancel := context.WithTimeout(rootCtx, 30*time.Second)
			defer cancel()

			t.logger.WithFields(logrus.Fields{
				"action":   "broadcast_abort_transaction",
				"duration": 30 * time.Second,
			}).Debug("context.WithTimeout")

			if err := t.client.AbortTransaction(ctx, host, tx); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		}, host)
	}

	return eg.Wait()
}

func (t *TxBroadcaster) BroadcastCommitTransaction(rootCtx context.Context, tx *Transaction) error {
	if !tx.TolerateNodeFailures {
		if err := t.ideal.Validate(); err != nil {
			return fmt.Errorf("tx does not tolerate node failures: %w", err)
		}
	}
	eg := enterrors.NewErrorGroupWrapper(t.logger)
	for _, host := range t.state.Hostnames() {
		// make sure we don't block forever if the caller passes in an unlimited
		// context. If another node does not respond within the timeout, consider
		// the tx commit attempt failed.
		ctx, cancel := context.WithTimeout(rootCtx, 30*time.Second)
		defer cancel()

		t.logger.WithFields(logrus.Fields{
			"action":   "broadcast_commit_transaction",
			"duration": 30 * time.Second,
		}).Debug("context.WithTimeout")

		host := host // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := t.client.CommitTransaction(ctx, host, tx); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		}, host)
	}

	return eg.Wait()
}

func copyTx(in *Transaction) *Transaction {
	out := *in
	return &out
}
