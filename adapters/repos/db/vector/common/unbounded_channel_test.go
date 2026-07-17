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

package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMakeUnboundedChannel(t *testing.T) {
	ch := MakeUnboundedChannel[int]()

	var j int
	for i := 0; i < 2000; i += 2 {
		// two writes per read
		ch.Push(i)
		ch.Push(i + 1)

		v := <-ch.Out()
		require.Equal(t, j, v)
		j++
	}

	for range 1000 {
		v := <-ch.Out()
		require.Equal(t, j, v)
		j++
	}

	require.Zero(t, ch.Len())

	ch.Close(t.Context())
}

func TestMakeUnboundedChannel_CapShrink(t *testing.T) {
	ch := MakeUnboundedChannel[int]()

	// add over 1024 items
	for i := range 2000 {
		ch.Push(i)
	}

	ch.mu.RLock()
	c := cap(ch.q)
	ch.mu.RUnlock()
	require.GreaterOrEqual(t, c, 2000)

	// drain almost all items so the internal buffer becomes heavily underused
	for i := range 1900 {
		v := <-ch.Out()
		require.Equal(t, i, v)
	}

	// pop() shrinks the internal buffer lazily on the background goroutine,
	// so the capacity settles asynchronously after the reads above. Poll
	// until it has shrunk back down.
	require.Eventually(t, func() bool {
		ch.mu.RLock()
		defer ch.mu.RUnlock()
		return cap(ch.q) <= 1024
	}, 5*time.Second, time.Millisecond)

	ch.Close(t.Context())
}

func BenchmarkUnboundedChannel(b *testing.B) {
	b.Run("Push", func(b *testing.B) {
		ch := MakeUnboundedChannel[int]()
		for b.Loop() {
			ch.Push(1)
		}
		b.StopTimer()
		ctx, cancel := context.WithCancel(b.Context())
		cancel()
		ch.Close(ctx)
	})

	b.Run("InThenOut", func(b *testing.B) {
		ch := MakeUnboundedChannel[int]()

		for b.Loop() {
			for j := range 1000 {
				ch.Push(j)
			}
			for range 1000 {
				<-ch.Out()
			}
		}
		b.StopTimer()
		ctx, cancel := context.WithCancel(b.Context())
		cancel()
		ch.Close(ctx)
	})

	b.Run("STD/InThenOut", func(b *testing.B) {
		ch := make(chan int, 1000)

		for b.Loop() {
			for j := range 1000 {
				ch <- j
			}
			for range 1000 {
				<-ch
			}
		}
	})
}
