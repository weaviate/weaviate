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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuccessfulOutgoingWriteTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := newTestTxManager()

	tx, err := man.BeginTransaction(ctx, trType, payload, 0)
	require.Nil(t, err)

	err = man.CommitWriteTransaction(ctx, tx)
	require.Nil(t, err)
}

func TestTryingToOpenTwoTransactions(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := newTestTxManager()

	tx1, err := man.BeginTransaction(ctx, trType, payload, 0)
	require.Nil(t, err)

	tx2, err := man.BeginTransaction(ctx, trType, payload, 0)
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

	man := newTestTxManager()

	tx1, err := man.BeginTransaction(ctx, trType, payload, 0)
	require.Nil(t, err)

	invalidTx := &Transaction{ID: "invalid"}

	err = man.CommitWriteTransaction(ctx, invalidTx)
	require.NotNil(t, err)
	assert.Equal(t, "invalid transaction", err.Error())

	err = man.CommitWriteTransaction(ctx, tx1)
	assert.Nil(t, err, "original transaction can still be committed")
}

func TestTryingToCommitTransactionPastTTL(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := newTestTxManager()

	tx1, err := man.BeginTransaction(ctx, trType, payload, time.Microsecond)
	require.Nil(t, err)

	expiredTx := &Transaction{ID: tx1.ID}

	// give the cancel handler some time to run
	time.Sleep(50 * time.Millisecond)

	err = man.CommitWriteTransaction(ctx, expiredTx)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "transaction TTL")

	// make sure it is possible to open future transactions
	_, err = man.BeginTransaction(context.Background(), trType, payload, 0)
	require.Nil(t, err)
}

func TestTryingToCommitIncomingTransactionPastTTL(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := newTestTxManager()

	dl := time.Now().Add(1 * time.Microsecond)

	tx := &Transaction{
		ID:       "123456",
		Type:     trType,
		Payload:  payload,
		Deadline: dl,
	}

	man.IncomingBeginTransaction(context.Background(), tx)

	// give the cancel handler some time to run
	time.Sleep(50 * time.Millisecond)

	err := man.IncomingCommitTransaction(ctx, tx)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "transaction TTL")

	// make sure it is possible to open future transactions
	_, err = man.BeginTransaction(context.Background(), trType, payload, 0)
	require.Nil(t, err)
}

func TestLettingATransactionExpire(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := newTestTxManager()

	tx1, err := man.BeginTransaction(ctx, trType, payload, time.Microsecond)
	require.Nil(t, err)

	// give the cancel handler some time to run
	time.Sleep(50 * time.Millisecond)

	// try to open a new one
	_, err = man.BeginTransaction(context.Background(), trType, payload, 0)
	require.Nil(t, err)

	// since the old one expired, we now expect a TTL error instead of a
	// concurrent tx error when trying to refer to the old one
	err = man.CommitWriteTransaction(context.Background(), tx1)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "transaction TTL")
}

func TestRemoteDoesntAllowOpeningTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()
	broadcaster := &fakeBroadcaster{
		openErr: ErrConcurrentTransaction,
	}

	man := newTestTxManagerWithRemote(broadcaster)

	tx1, err := man.BeginTransaction(ctx, trType, payload, 0)
	require.Nil(t, tx1)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "open transaction")

	assert.Len(t, broadcaster.abortCalledId, 36, "a valid uuid was aborted")
}

func TestRemoteDoesntAllowOpeningTransactionAbortFails(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()
	broadcaster := &fakeBroadcaster{
		openErr:  ErrConcurrentTransaction,
		abortErr: fmt.Errorf("cannot abort"),
	}

	man, hook := newTestTxManagerWithRemoteLoggerHook(broadcaster)

	tx1, err := man.BeginTransaction(ctx, trType, payload, 0)
	require.Nil(t, tx1)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "open transaction")

	assert.Len(t, broadcaster.abortCalledId, 36, "a valid uuid was aborted")

	require.Len(t, hook.Entries, 1)
	assert.Equal(t, "broadcast tx abort failed", hook.Entries[0].Message)
}

type fakeBroadcaster struct {
	openErr       error
	commitErr     error
	abortErr      error
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
	return f.abortErr
}

func (f *fakeBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *Transaction,
) error {
	return f.commitErr
}

func TestSuccessfulDistributedWriteTransaction(t *testing.T) {
	ctx := context.Background()

	var remoteState interface{}
	remote := newTestTxManager()
	remote.SetCommitFn(func(ctx context.Context, tx *Transaction) error {
		remoteState = tx.Payload
		return nil
	})
	local := NewTxManager(&wrapTxManagerAsBroadcaster{remote},
		&fakeTxPersistence{}, remote.logger)
	local.StartAcceptIncoming()

	payload := "my-payload"
	trType := TransactionType("my-type")

	tx, err := local.BeginTransaction(ctx, trType, payload, 0)
	require.Nil(t, err)

	err = local.CommitWriteTransaction(ctx, tx)
	require.Nil(t, err)

	assert.Equal(t, "my-payload", remoteState)
}

func TestConcurrentDistributedTransaction(t *testing.T) {
	ctx := context.Background()

	var remoteState interface{}
	remote := newTestTxManager()
	remote.SetCommitFn(func(ctx context.Context, tx *Transaction) error {
		remoteState = tx.Payload
		return nil
	})
	local := NewTxManager(&wrapTxManagerAsBroadcaster{remote},
		&fakeTxPersistence{}, remote.logger)

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
	_, err := remote.BeginTransaction(ctx, trType, "wrong payload", 0)
	require.Nil(t, err)

	tx, err := local.BeginTransaction(ctx, trType, payload, 0)
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
	node1 := newTestTxManagerWithRemote(broadcaster)
	node2 := newTestTxManagerWithRemote(broadcaster)
	node3 := newTestTxManagerWithRemote(broadcaster)

	broadcaster.nodes = []*TxManager{node1, node2, node3}

	trType := TransactionType("my-type")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := node1.BeginTransaction(ctx, trType, "payload-from-node-1", 0)
		assert.NotNil(t, err, "open tx 1 must fail")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := node2.BeginTransaction(ctx, trType, "payload-from-node-2", 0)
		assert.NotNil(t, err, "open tx 2 must fail")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := node3.BeginTransaction(ctx, trType, "payload-from-node-3", 0)
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
	_, err := w.txManager.IncomingBeginTransaction(ctx, tx)
	return err
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
		if _, err := node.IncomingBeginTransaction(ctx, tx); err != nil {
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

func TestSuccessfulDistributedReadTransaction(t *testing.T) {
	ctx := context.Background()
	payload := "my-payload"

	remote := newTestTxManager()
	remote.SetResponseFn(func(ctx context.Context, tx *Transaction) ([]byte, error) {
		tx.Payload = payload
		return nil, nil
	})
	local := NewTxManager(&wrapTxManagerAsBroadcaster{remote},
		&fakeTxPersistence{}, remote.logger)
	// TODO local.SetConsensusFn

	trType := TransactionType("my-read-tx")

	tx, err := local.BeginTransaction(ctx, trType, nil, 0)
	require.Nil(t, err)

	local.CloseReadTransaction(ctx, tx)

	assert.Equal(t, "my-payload", tx.Payload)
}

func TestSuccessfulDistributedTransactionSetAllowUnready(t *testing.T) {
	ctx := context.Background()
	payload := "my-payload"

	types := []TransactionType{"type0", "type1"}
	remote := newTestTxManagerAllowUnready(types)
	remote.SetResponseFn(func(ctx context.Context, tx *Transaction) ([]byte, error) {
		tx.Payload = payload
		return nil, nil
	})
	local := NewTxManager(&wrapTxManagerAsBroadcaster{remote},
		&fakeTxPersistence{}, remote.logger)
	local.SetAllowUnready(types)

	trType := TransactionType("my-read-tx")

	tx, err := local.BeginTransaction(ctx, trType, nil, 0)
	require.Nil(t, err)

	local.CloseReadTransaction(ctx, tx)

	assert.ElementsMatch(t, types, remote.allowUnready)
	assert.ElementsMatch(t, types, local.allowUnready)
	assert.Equal(t, "my-payload", tx.Payload)
}

func TestTxWithDeadline(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		payload := "my-payload"
		trType := TransactionType("my-type")

		ctx := context.Background()

		man := newTestTxManager()

		tx, err := man.BeginTransaction(ctx, trType, payload, 1*time.Nanosecond)
		require.Nil(t, err)

		ctx, cancel := context.WithDeadline(context.Background(), tx.Deadline)
		defer cancel()

		assert.NotNil(t, ctx.Err())
	})

	t.Run("still valid", func(t *testing.T) {
		payload := "my-payload"
		trType := TransactionType("my-type")

		ctx := context.Background()

		man := newTestTxManager()

		tx, err := man.BeginTransaction(ctx, trType, payload, 10*time.Second)
		require.Nil(t, err)

		ctx, cancel := context.WithDeadline(context.Background(), tx.Deadline)
		defer cancel()

		assert.Nil(t, ctx.Err())
	})
}

func newTestTxManager() *TxManager {
	logger, _ := test.NewNullLogger()
	m := NewTxManager(&fakeBroadcaster{}, &fakeTxPersistence{}, logger)
	m.StartAcceptIncoming()
	return m
}

func newTestTxManagerWithRemote(remote Remote) *TxManager {
	logger, _ := test.NewNullLogger()
	m := NewTxManager(remote, &fakeTxPersistence{}, logger)
	m.StartAcceptIncoming()
	return m
}

func newTestTxManagerWithRemoteLoggerHook(remote Remote) (*TxManager, *test.Hook) {
	logger, hook := test.NewNullLogger()
	m := NewTxManager(remote, &fakeTxPersistence{}, logger)
	m.StartAcceptIncoming()
	return m, hook
}

func newTestTxManagerAllowUnready(types []TransactionType) *TxManager {
	logger, _ := test.NewNullLogger()
	m := NewTxManager(&fakeBroadcaster{}, &fakeTxPersistence{}, logger)
	m.SetAllowUnready(types)
	m.StartAcceptIncoming()
	return m
}

// does nothing as these do not involve crashes
type fakeTxPersistence struct{}

func (f *fakeTxPersistence) StoreTx(ctx context.Context,
	tx *Transaction,
) error {
	return nil
}

func (f *fakeTxPersistence) DeleteTx(ctx context.Context,
	txID string,
) error {
	return nil
}

func (f *fakeTxPersistence) IterateAll(ctx context.Context,
	cb func(tx *Transaction),
) error {
	return nil
}
