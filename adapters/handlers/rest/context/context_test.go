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

package context

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchNamespaceSlot(t *testing.T) {
	t.Run("set then get round-trips", func(t *testing.T) {
		ctx, slot := WithBatchNamespaceSlot(context.Background())
		require.Empty(t, slot.Namespace, "a fresh slot holds no namespace")

		SetBatchNamespace(ctx, "ns_a")

		assert.Equal(t, "ns_a", slot.Namespace)
	})

	t.Run("the slot survives a derived context", func(t *testing.T) {
		// Handlers derive from the request context repeatedly before the
		// namespace is known; the slot is a pointer, so the write is still
		// visible to the middleware that installed it.
		ctx, slot := WithBatchNamespaceSlot(context.Background())
		derived := AddPrincipalToContext(ctx, nil)

		SetBatchNamespace(derived, "ns_a")

		assert.Equal(t, "ns_a", slot.Namespace)
	})

	t.Run("set without a slot is a no-op", func(t *testing.T) {
		assert.NotPanics(t, func() { SetBatchNamespace(context.Background(), "ns_a") })
	})
}
