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

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type TxBroadcaster struct {
	state  MemberLister
	client Client
}

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

func (t *TxBroadcaster) BroadcastTransaction(ctx context.Context, tx *Transaction) error {
	// TODO health check
	// it should be impossible to even attempt to open a transaction if we
	// already know that the cluster is not healthy

	eg := &errgroup.Group{}
	for _, host := range t.state.Hostnames() {
		host := host // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := t.client.OpenTransaction(ctx, host, tx); err != nil {
				return errors.Wrapf(err, "host %q", host)
			}

			return nil
		})
	}

	return eg.Wait()
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
