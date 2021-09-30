package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuccesfulOutgoingTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := NewTxManager(&fakeBroadcaster{})

	tx, err := man.BeginTransaction(ctx, trType, payload)
	require.Nil(t, err)

	err = man.CommitTransaction(ctx, tx)
	require.Nil(t, err)
}

func TestTryingToOpenTwoTransactions(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := NewTxManager(&fakeBroadcaster{})

	tx1, err := man.BeginTransaction(ctx, trType, payload)
	require.Nil(t, err)

	tx2, err := man.BeginTransaction(ctx, trType, payload)
	assert.Nil(t, tx2)
	require.NotNil(t, err)
	assert.Equal(t, "concurrent transaction", err.Error())

	err = man.CommitTransaction(ctx, tx1)
	assert.Nil(t, err, "original transaction can still be committed")
}

func TestTryingToCommitInvalidTransaction(t *testing.T) {
	payload := "my-payload"
	trType := TransactionType("my-type")
	ctx := context.Background()

	man := NewTxManager(&fakeBroadcaster{})

	tx1, err := man.BeginTransaction(ctx, trType, payload)
	require.Nil(t, err)

	invalidTx := &Transaction{ID: "invalid"}

	err = man.CommitTransaction(ctx, invalidTx)
	require.NotNil(t, err)
	assert.Equal(t, "invalid transaction", err.Error())

	err = man.CommitTransaction(ctx, tx1)
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

	tx1, err := man.BeginTransaction(ctx, trType, payload)
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
	tx *Transaction) error {
	return f.openErr
}

func (f *fakeBroadcaster) BroadcastAbortTransaction(ctx context.Context,
	tx *Transaction) error {
	f.abortCalledId = tx.ID
	return nil
}

func (f *fakeBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *Transaction) error {
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

	tx, err := local.BeginTransaction(ctx, trType, payload)
	require.Nil(t, err)

	err = local.CommitTransaction(ctx, tx)
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
	_, err := remote.BeginTransaction(ctx, trType, "wrong payload")
	require.Nil(t, err)

	tx, err := local.BeginTransaction(ctx, trType, payload)
	require.Nil(t, tx)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "concurrent transaction")

	assert.Equal(t, nil, remoteState, "remote state should not have been updated")
}

type wrapTxManagerAsBroadcaster struct {
	txManager *TxManager
}

func (w *wrapTxManagerAsBroadcaster) BroadcastTransaction(ctx context.Context,
	tx *Transaction) error {
	return w.txManager.IncomingBeginTransaction(ctx, tx)
}

func (w *wrapTxManagerAsBroadcaster) BroadcastAbortTransaction(ctx context.Context,
	tx *Transaction) error {
	w.txManager.IncomingAbortTransaction(ctx, tx)
	return nil
}

func (w *wrapTxManagerAsBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *Transaction) error {
	return w.txManager.IncomingCommitTransaction(ctx, tx)
}
