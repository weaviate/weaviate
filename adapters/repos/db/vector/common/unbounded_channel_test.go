//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"context"
	"testing"

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
