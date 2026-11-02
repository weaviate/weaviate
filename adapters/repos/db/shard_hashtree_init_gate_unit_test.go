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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func TestWaitForMinimalHashTreeInitializationGate(t *testing.T) {
	const returnTimeout = 2 * time.Second
	const parkProbe = 100 * time.Millisecond

	openCh := func() chan struct{} { return make(chan struct{}) }
	closedCh := func() chan struct{} {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	cancelledCtx := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	tests := []struct {
		name             string
		tree             bool
		fullyInitialized bool
		ch               func() chan struct{}
		ctx              func() context.Context
		blocksUntilClose bool
		wantErr          error
	}{
		{name: "treeNil"},
		{name: "treeNilOpenChannel", ch: openCh},
		{name: "fullyInitializedOpenChannel", tree: true, fullyInitialized: true, ch: openCh},
		{name: "channelNilNotInitialized", tree: true},
		{name: "channelClosed", tree: true, ch: closedCh},
		{name: "channelOpenBlocksUntilClosed", tree: true, ch: openCh, blocksUntilClose: true},
		{name: "ctxCancelledChannelOpen", tree: true, ch: openCh, ctx: cancelledCtx, wantErr: context.Canceled},
		{name: "ctxCancelledChannelNil", tree: true, ctx: cancelledCtx},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{hashtreeFullyInitialized: tc.fullyInitialized}
			if tc.tree {
				ht, err := hashtree.NewHashTree(1)
				require.NoError(t, err)
				s.hashtree = ht
			}
			var ch chan struct{}
			if tc.ch != nil {
				ch = tc.ch()
			}
			s.minimalHashtreeInitializationCh = ch
			ctx := context.Background()
			if tc.ctx != nil {
				ctx = tc.ctx()
			}

			done := make(chan error, 1)
			go func() { done <- s.waitForMinimalHashTreeInitialization(ctx) }()

			if tc.blocksUntilClose {
				select {
				case <-done:
					t.Fatal("returned while the gate was armed")
				case <-time.After(parkProbe):
				}
				close(ch)
			}

			select {
			case err := <-done:
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(returnTimeout):
				t.Fatalf("waitForMinimalHashTreeInitialization did not return within %s", returnTimeout)
			}
		})
	}
}
