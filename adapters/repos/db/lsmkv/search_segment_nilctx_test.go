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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
)

// The WAND scoring loops must tolerate a non-cancellable context the way
// DoBlockMaxAnd does. A nil ctx (or one whose Done() returns nil, like
// Background/TODO) must not panic — neither on the periodic cancellation check
// (ctx.Done) nor on the slow-query annotation (ctx.Value via
// AnnotateSlowQueryLog) reached on the empty-results early return.
func TestWandLoops_NonCancellableContext(t *testing.T) {
	ctxs := []struct {
		name string
		ctx  context.Context
	}{
		{name: "nil", ctx: nil},
		{name: "background", ctx: context.Background()},
		{name: "todo", ctx: context.TODO()},
	}

	for _, c := range ctxs {
		t.Run("DoBlockMaxWand/"+c.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				topKHeap, err := DoBlockMaxWand(c.ctx, 10, Terms{}, 1.0, false, 0, 0, logrus.New())
				require.NoError(t, err)
				require.NotNil(t, topKHeap)
			})
		})

		t.Run("DoWand/"+c.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				topKHeap := DoWand(c.ctx, 10, &terms.Terms{}, 1.0, false, 0, logrus.New())
				require.NotNil(t, topKHeap)
			})
		})
	}
}
