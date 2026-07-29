//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloseReadTransaction_NoCurrentTransaction pins the `currentTransaction ==
// nil` guard: with no open transaction the call must be rejected, never
// dereferenced (a `!= nil` mutant would fall through to currentTransaction.ID
// and panic).
func TestCloseReadTransaction_NoCurrentTransaction(t *testing.T) {
	m := newTestTxManagerWithRemote(&fakeBroadcaster{})
	err := m.CloseReadTransaction(context.Background(), &Transaction{ID: "ghost"})
	assert.ErrorIs(t, err, ErrInvalidTransaction)
}

// TestCloseReadTransaction_MismatchedID pins the `currentTransaction.ID != tx.ID`
// guard: closing with a different transaction ID must be rejected.
func TestCloseReadTransaction_MismatchedID(t *testing.T) {
	m := newTestTxManagerWithRemote(&fakeBroadcaster{})
	m.currentTransaction = &Transaction{ID: "open-tx"}
	err := m.CloseReadTransaction(context.Background(), &Transaction{ID: "other-tx"})
	assert.ErrorIs(t, err, ErrInvalidTransaction)
}

// TestCloseReadTransaction_CommitBroadcastError pins the `err != nil` guard on
// the commit broadcast: a broadcast failure must surface as an error.
func TestCloseReadTransaction_CommitBroadcastError(t *testing.T) {
	m := newTestTxManagerWithRemote(&fakeBroadcaster{commitErr: errors.New("commit boom")})
	m.currentTransaction = &Transaction{ID: "open-tx"}

	err := m.CloseReadTransaction(context.Background(), &Transaction{ID: "open-tx"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit boom")
}

// TestCloseReadTransaction_AbortBroadcastErrorLogged pins the `err != nil` guard
// on the fallback abort broadcast: when both commit and abort fail, the abort
// failure must be logged.
func TestCloseReadTransaction_AbortBroadcastErrorLogged(t *testing.T) {
	m, hook := newTestTxManagerWithRemoteLoggerHook(&fakeBroadcaster{
		commitErr: errors.New("commit boom"),
		abortErr:  errors.New("abort boom"),
	})
	m.currentTransaction = &Transaction{ID: "open-tx"}

	err := m.CloseReadTransaction(context.Background(), &Transaction{ID: "open-tx"})
	require.Error(t, err)

	var logged bool
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "broadcast tx (read-only) abort failed") {
			logged = true
			break
		}
	}
	assert.True(t, logged, "a failed abort broadcast must be logged")
}
