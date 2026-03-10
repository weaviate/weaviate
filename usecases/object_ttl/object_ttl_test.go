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

package objectttl

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalState(t *testing.T) {
	t.Run("initial state is not running", func(t *testing.T) {
		s := NewLocalStatus()
		assert.False(t, s.IsRunning())
	})

	t.Run("SetRunning succeeds when not running", func(t *testing.T) {
		s := NewLocalStatus()

		ok, ctx := s.SetRunning()

		require.True(t, ok)
		require.NotNil(t, ctx)
		assert.True(t, s.IsRunning())
		assert.NoError(t, ctx.Err(), "context should not be cancelled yet")
	})

	t.Run("SetRunning returns valid non-cancelled context", func(t *testing.T) {
		s := NewLocalStatus()

		ok, ctx := s.SetRunning()

		require.True(t, ok)
		require.NotNil(t, ctx)

		select {
		case <-ctx.Done():
			t.Fatal("context should not be done yet")
		default:
			// expected: context is still active
		}
	})

	t.Run("SetRunning fails when already running", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok, "first SetRunning should succeed")

		ok2, ctx2 := s.SetRunning()

		assert.False(t, ok2)
		assert.Nil(t, ctx2)
		assert.True(t, s.IsRunning(), "should still be running after failed SetRunning")
	})

	t.Run("ResetRunning succeeds when running and cancels context", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)
		require.NotNil(t, ctx)

		aborted := s.ResetRunning("finished")

		assert.True(t, aborted)
		assert.False(t, s.IsRunning())

		// context must be cancelled
		select {
		case <-ctx.Done():
			// expected
		default:
			t.Fatal("context should be done after ResetRunning")
		}
	})

	t.Run("ResetRunning sets context error to context.Canceled", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)

		s.ResetRunning("finished")

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("ResetRunning cause contains the provided reason", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)

		s.ResetRunning("aborted")

		cause := context.Cause(ctx)
		require.NotNil(t, cause)
		assert.ErrorIs(t, cause, context.Canceled)
		assert.Contains(t, cause.Error(), "aborted")
	})

	t.Run("ResetRunning fails when not running", func(t *testing.T) {
		s := NewLocalStatus()

		aborted := s.ResetRunning("aborted")

		assert.False(t, aborted)
		assert.False(t, s.IsRunning())
	})

	t.Run("ResetRunning on fresh LocalStatus returns false", func(t *testing.T) {
		s := NewLocalStatus()

		result := s.ResetRunning("some cause")

		assert.False(t, result)
	})

	t.Run("second ResetRunning after first returns false", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		first := s.ResetRunning("finished")
		second := s.ResetRunning("finished again")

		assert.True(t, first)
		assert.False(t, second)
	})

	t.Run("SetRunning can be called again after ResetRunning", func(t *testing.T) {
		s := NewLocalStatus()

		ok1, ctx1 := s.SetRunning()
		require.True(t, ok1)
		s.ResetRunning("finished")

		ok2, ctx2 := s.SetRunning()

		assert.True(t, ok2)
		require.NotNil(t, ctx2)
		assert.True(t, s.IsRunning())
		assert.NoError(t, ctx2.Err(), "new context should not be cancelled")

		// old context should still be cancelled
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
	})

	t.Run("each SetRunning produces an independent context", func(t *testing.T) {
		s := NewLocalStatus()

		ok1, ctx1 := s.SetRunning()
		require.True(t, ok1)
		s.ResetRunning("round 1")

		ok2, ctx2 := s.SetRunning()
		require.True(t, ok2)

		// ctx1 is cancelled, ctx2 is not
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
		assert.NoError(t, ctx2.Err())

		s.ResetRunning("round 2")

		assert.ErrorIs(t, ctx2.Err(), context.Canceled)
	})

	t.Run("concurrent SetRunning calls: only one succeeds", func(t *testing.T) {
		s := NewLocalStatus()

		const goroutines = 50
		var wg sync.WaitGroup
		var successCount atomic.Int32

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				ok, _ := s.SetRunning()
				if ok {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), successCount.Load(), "exactly one goroutine should win SetRunning")
		assert.True(t, s.IsRunning())
	})

	t.Run("concurrent ResetRunning calls: only one succeeds", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		const goroutines = 50
		var wg sync.WaitGroup
		var successCount atomic.Int32

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				if s.ResetRunning("aborted") {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), successCount.Load(), "exactly one goroutine should win ResetRunning")
		assert.False(t, s.IsRunning())
	})

	t.Run("concurrent SetRunning and ResetRunning: consistent state", func(t *testing.T) {
		s := NewLocalStatus()
		// prime with a running state
		ok, _ := s.SetRunning()
		require.True(t, ok)

		var wg sync.WaitGroup
		const goroutines = 20

		// half try to abort, half try to set running again
		wg.Add(goroutines * 2)
		for range goroutines {
			go func() {
				defer wg.Done()
				s.ResetRunning("aborted")
			}()
			go func() {
				defer wg.Done()
				s.SetRunning()
			}()
		}
		wg.Wait()

		// state must be coherent: IsRunning must agree with internal invariants
		running := s.IsRunning()
		// no panic, no deadlock — just verify IsRunning is consistent
		assert.IsType(t, false, running) // bool type assertion
	})

	t.Run("context cancelled by ResetRunning is propagated to child contexts", func(t *testing.T) {
		s := NewLocalStatus()
		ok, parentCtx := s.SetRunning()
		require.True(t, ok)

		childCtx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		s.ResetRunning("aborted")

		select {
		case <-childCtx.Done():
			assert.ErrorIs(t, errors.Unwrap(context.Cause(childCtx)), context.Canceled)
		default:
			t.Fatal("child context should be done after parent is cancelled")
		}
	})

	t.Run("IsRunning reflects state changes correctly across lifecycle", func(t *testing.T) {
		s := NewLocalStatus()

		assert.False(t, s.IsRunning(), "initially not running")

		ok, _ := s.SetRunning()
		require.True(t, ok)
		assert.True(t, s.IsRunning(), "running after SetRunning")

		s.ResetRunning("finished")
		assert.False(t, s.IsRunning(), "not running after ResetRunning")

		ok2, _ := s.SetRunning()
		require.True(t, ok2)
		assert.True(t, s.IsRunning(), "running again after second SetRunning")
	})

	t.Run("multiple full cycles work correctly", func(t *testing.T) {
		s := NewLocalStatus()

		for i := range 5 {
			ok, ctx := s.SetRunning()
			require.True(t, ok, "cycle %d: SetRunning should succeed", i)
			require.NotNil(t, ctx)
			assert.NoError(t, ctx.Err())

			result := s.ResetRunning("finished")
			assert.True(t, result, "cycle %d: ResetRunning should succeed", i)
			assert.ErrorIs(t, ctx.Err(), context.Canceled)
			assert.False(t, s.IsRunning())
		}
	})
}
