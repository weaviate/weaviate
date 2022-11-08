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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuccesfulOutgoingTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := NewTxManager(&fakeBroadcaster{})

	tx, err := man.BeginWriteTransaction(ctx, trType, payload)
	require.Nil(t, err)

	err = man.CommitWriteTransaction(ctx, tx)
	require.Nil(t, err)
}

func TestTryingToOpenTwoTransactions(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := NewTxManager(&fakeBroadcaster{})

	tx1, err := man.BeginWriteTransaction(ctx, trType, payload)
	require.Nil(t, err)

	tx2, err := man.BeginWriteTransaction(ctx, trType, payload)
	assert.Nil(t, tx2)
	require.NotNil(t, err)
	assert.Equal(t, "concurrent transaction", err.Error())

	err = man.CommitWriteTransaction(ctx, tx1)
	assert.Nil(t, err, "original transaction can still be committed")
}

func TestTryingToCommitInvalidTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := NewTxManager(&fakeBroadcaster{})

	tx1, err := man.BeginWriteTransaction(ctx, trType, payload)
	require.Nil(t, err)

	invalidTx := &Transaction{ID: "invalid"}

	err = man.CommitWriteTransaction(ctx, invalidTx)
	require.NotNil(t, err)
	assert.Equal(t, "invalid transaction", err.Error())

	err = man.CommitWriteTransaction(ctx, tx1)
	assert.Nil(t, err, "original transaction can still be committed")
}

func TestRemoteDoesntAllowOpeningTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()
	broadcaster := &fakeBroadcaster{
		openErr: ErrConcurrentTransaction,
	}

	man := NewTxManager(broadcaster)

	tx1, err := man.BeginWriteTransaction(ctx, trType, payload)
	require.Nil(t, tx1)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "open transaction")

	assert.Len(t, broadcaster.abortCalledId, 36, "a valid uuid was aborted")
}

type fakeBroadcaster struct {
	openErr       error
	commitErr     error
	abortCalledId string
}

func (f *fakeBroadcaster) BroadcastTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return f.openErr
}

func (f *fakeBroadcaster) BroadcastAbortTransaction(ctx context.Context,
	tx *Transaction,
) error {
	f.abortCalledId = tx.ID
	return nil
}

func (f *fakeBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return f.commitErr
}

func TestSuccessfulDistributedTransaction(t *testing.T) {
	ctx := context.Background()

	var remoteState interface{}
	remote := NewTxManager(&fakeBroadcaster{})
	remote.SetCommitFn(func(ctx context.Context, tx *Transaction) error {
		remoteState = tx.Payload
		return nil
	})
	local := NewTxManager(&wrapTxManagerAsBroadcaster{remote})

	payload := "my-payload"
	trType := TransactionType("my-type")

	tx, err := local.BeginWriteTransaction(ctx, trType, payload)
	require.Nil(t, err)

	err = local.CommitWriteTransaction(ctx, tx)
	require.Nil(t, err)

	assert.Equal(t, "my-payload", remoteState)
}

func TestConcurrentDistributedTransaction(t *testing.T) {
	ctx := context.Background()

	var remoteState interface{}
	remote := NewTxManager(&fakeBroadcaster{})
	remote.SetCommitFn(func(ctx context.Context, tx *Transaction) error {
		remoteState = tx.Payload
		return nil
	})
	local := NewTxManager(&wrapTxManagerAsBroadcaster{remote})

	payload := "my-payload"
	trType := TransactionType("my-type")

	// open a transaction on the remote to simulate a concurrent transaction.
	// Since it uses the fakeBroadcaster it does not tell anyone about it, this
	// way we can be sure that the reason for failure is actually a concurrent
	// transaction on the remote side, not on the local side. Compare this to a
	// situation where broadcasting was bi-directional: Then this transaction
	// would have been opened successfully and already be replicated to the
	// "local" tx manager. So the next call on "local" would also fail, but for
	// the wrong reason: It would fail because another transaction is already in
	// place. We, however want to simulate a situation where due to network
	// delays, etc. both sides try to open a transaction more or less in
	// parallel.
	_, err := remote.BeginWriteTransaction(ctx, trType, "wrong payload")
	require.Nil(t, err)

	tx, err := local.BeginWriteTransaction(ctx, trType, payload)
	require.Nil(t, tx)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "concurrent transaction")

	assert.Equal(t, nil, remoteState, "remote state should not have been updated")
}

// This test simulates three nodes trying to open a tx at basically the same
// time with the simulated network being so slow that other nodes will try to
// open their own transactions before they receive the incoming tx. This is a
// situation where everyone thinks they were the first to open the tx and there
// is no clear winner. All attempts must fail!
func TestConcurrentOpenAttemptsOnSlowNetwork(t *testing.T) {
	ctx := context.Background()

	broadcaster := &slowMultiBroadcaster{delay: 100 * time.Millisecond}
	node1 := NewTxManager(broadcaster)
	node2 := NewTxManager(broadcaster)
	node3 := NewTxManager(broadcaster)

	broadcaster.nodes = []*TxManager{node1, node2, node3}

	trType := TransactionType("my-type")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := node1.BeginWriteTransaction(ctx, trType, "payload-from-node-1")
		assert.NotNil(t, err, "open tx 1 must fail")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := node2.BeginWriteTransaction(ctx, trType, "payload-from-node-2")
		assert.NotNil(t, err, "open tx 2 must fail")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := node3.BeginWriteTransaction(ctx, trType, "payload-from-node-3")
		assert.NotNil(t, err, "open tx 3 must fail")
	}()

	wg.Wait()
}

type wrapTxManagerAsBroadcaster struct {
	txManager *TxManager
}

func (w *wrapTxManagerAsBroadcaster) BroadcastTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return w.txManager.IncomingBeginTransaction(ctx, tx)
}

func (w *wrapTxManagerAsBroadcaster) BroadcastAbortTransaction(ctx context.Context,
	tx *Transaction,
) error {
	w.txManager.IncomingAbortTransaction(ctx, tx)
	return nil
}

func (w *wrapTxManagerAsBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return w.txManager.IncomingCommitTransaction(ctx, tx)
}

type slowMultiBroadcaster struct {
	delay time.Duration
	nodes []*TxManager
}

func (b *slowMultiBroadcaster) BroadcastTransaction(ctx context.Context,
	tx *Transaction,
) error {
	time.Sleep(b.delay)
	for _, node := range b.nodes {
		if err := node.IncomingBeginTransaction(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (b *slowMultiBroadcaster) BroadcastAbortTransaction(ctx context.Context,
	tx *Transaction,
) error {
	time.Sleep(b.delay)
	for _, node := range b.nodes {
		node.IncomingAbortTransaction(ctx, tx)
	}

	return nil
}

func (b *slowMultiBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *Transaction,
) error {
	time.Sleep(b.delay)
	for _, node := range b.nodes {
		if err := node.IncomingCommitTransaction(ctx, tx); err != nil {
			return err
		}
	}

	return nil
}
