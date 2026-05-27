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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBeginTransaction_ZeroTTLIsUnlimited pins the `ttl > 0` guard: a zero TTL
// means "unlimited" and must be encoded as the Unix epoch sentinel, not as a
// deadline of now (which a `ttl >= 0` mutant would produce, expiring the tx
// immediately).
func TestBeginTransaction_ZeroTTLIsUnlimited(t *testing.T) {
	m := newTestTxManagerWithRemote(&fakeBroadcaster{})

	tx, err := m.BeginTransaction(context.Background(), TransactionType("my-type"), "payload", 0)
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(0), tx.Deadline)
}

// TestIncomingAbortTransaction_MismatchedIDIsNoop pins the
// `currentTransaction.ID != tx.ID` guard: an abort for a different transaction
// must be ignored, leaving the in-flight transaction untouched.
func TestIncomingAbortTransaction_MismatchedIDIsNoop(t *testing.T) {
	m := newTestTxManagerWithRemote(&fakeBroadcaster{})
	open := &Transaction{ID: "open-tx"}
	m.currentTransaction = open

	m.IncomingAbortTransaction(context.Background(), &Transaction{ID: "other-tx"})

	assert.Same(t, open, m.currentTransaction, "a mismatched abort must be a no-op")
}
