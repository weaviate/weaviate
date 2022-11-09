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
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type TxBroadcaster struct {
	state       MemberLister
	client      Client
	consensusFn ConsensusFn
}

// The Broadcaster is the link between the the current node and all other nodes
// during a tx operation. This makes it a natural place to inject a consensus
// function for read transactions. How consensus is reached is completely opaque
// to the broadcaster and can be controlled through custom business logic.
type ConsensusFn func(in []*Transaction) (*Transaction, error)

type Client interface {
	OpenTransaction(ctx context.Context, host string, tx *Transaction) error
	AbortTransaction(ctx context.Context, host string, tx *Transaction) error
	CommitTransaction(ctx context.Context, host string, tx *Transaction) error
}

type MemberLister interface {
	Hostnames() []string
}

func NewTxBroadcaster(state MemberLister, client Client) *TxBroadcaster {
	return &TxBroadcaster{
		state:  state,
		client: client,
	}
}

func (t *TxBroadcaster) SetConsensusFunction(fn ConsensusFn) {
	t.consensusFn = fn
}

func (t *TxBroadcaster) BroadcastTransaction(ctx context.Context, tx *Transaction) error {
	// TODO health check
	// it should be impossible to even attempt to open a transaction if we
	// already know that the cluster is not healthy

	hosts := t.state.Hostnames()
	resTx := make([]*Transaction, len(hosts))
	eg := &errgroup.Group{}
	for i, host := range hosts {
		i := i       // https://golang.org/doc/faq#closures_and_goroutines
		host := host // https://golang.org/doc/faq#closures_and_goroutines

		eg.Go(func() error {
			// the client call can mutate the tx, so we need to work with copies to
			// prevent a race and to be able to keep all individual results, so they
			// can be passed to the consensus fn
			resTx[i] = copyTx(tx)
			if err := t.client.OpenTransaction(ctx, host, resTx[i]); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		return err
	}

	if t.consensusFn != nil {
		merged, err := t.consensusFn(resTx)
		if err != nil {
			return fmt.Errorf("try to reach consenus: %w", err)
		}

		tx.Payload = merged.Payload
	}

	return nil
}

func (t *TxBroadcaster) BroadcastAbortTransaction(ctx context.Context, tx *Transaction) error {
	eg := &errgroup.Group{}
	for _, host := range t.state.Hostnames() {
		host := host // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := t.client.AbortTransaction(ctx, host, tx); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		})
	}

	return eg.Wait()
}

func (t *TxBroadcaster) BroadcastCommitTransaction(ctx context.Context, tx *Transaction) error {
	eg := &errgroup.Group{}
	for _, host := range t.state.Hostnames() {
		host := host // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := t.client.CommitTransaction(ctx, host, tx); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		})
	}

	return eg.Wait()
}

func copyTx(in *Transaction) *Transaction {
	out := &Transaction{
		ID:      in.ID,
		Type:    in.Type,
		Payload: in.Payload,
	}

	return out
}
