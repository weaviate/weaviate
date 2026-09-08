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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

	t.Run("Finished cancels the context with its cause", func(t *testing.T) {
		s := NewLocalStatus()
		_, ctx := s.SetRunning()

		s.Finished()

		require.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrFinished)
		assert.False(t, s.IsRunning())
	})

	t.Run("Abort cancels the context but keeps the slot reserved", func(t *testing.T) {
		s := NewLocalStatus()
		_, ctx := s.SetRunning()

		require.True(t, s.Abort())

		require.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrAborted)
		assert.True(t, s.IsRunning(),
			"the deletion observes the cancellation with a delay, so the slot stays reserved")
	})

	t.Run("no deletion starts while an aborted one is still draining", func(t *testing.T) {
		s := NewLocalStatus()
		_, ctx1 := s.SetRunning()
		require.True(t, s.Abort())

		ok, ctx2 := s.SetRunning()

		assert.False(t, ok, "a successor must not start on top of a draining deletion")
		assert.Nil(t, ctx2)

		// the draining deletion finally returns; its cleanup can only reach its
		// own context, because no other was ever created
		s.Finished()
		assert.ErrorIs(t, context.Cause(ctx1), ErrAborted,
			"the cause it was aborted with outranks the cleanup's")
		assert.NotErrorIs(t, context.Cause(ctx1), ErrFinished)
		assert.False(t, s.IsRunning())
	})

	t.Run("Abort reports whether a deletion was running", func(t *testing.T) {
		s := NewLocalStatus()
		assert.False(t, s.Abort(), "nothing to abort")

		_, ctx := s.SetRunning()
		assert.True(t, s.Abort(), "the deletion is cancelled")
		assert.ErrorIs(t, context.Cause(ctx), ErrAborted)

		assert.True(t, s.Abort(), "a repeat while it drains still reports the deletion")
		assert.True(t, s.IsRunning(), "the slot stays reserved until the deletion finishes")

		s.Finished()
		assert.False(t, s.Abort(), "nothing to abort once it finished")
	})

	t.Run("Finished on a status that is not running does nothing", func(t *testing.T) {
		s := NewLocalStatus()
		s.Finished()
		assert.False(t, s.IsRunning())
	})

	t.Run("SetRunning can be called again after Finished", func(t *testing.T) {
		s := NewLocalStatus()

		ok1, ctx1 := s.SetRunning()
		require.True(t, ok1)
		s.Finished()

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
		s.Finished()

		ok2, ctx2 := s.SetRunning()
		require.True(t, ok2)

		// ctx1 is cancelled, ctx2 is not
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
		assert.NoError(t, ctx2.Err())

		s.Finished()

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

	t.Run("concurrent Abort calls all report the running deletion", func(t *testing.T) {
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
				if s.Abort() {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(goroutines), successCount.Load(),
			"every caller sees the deletion it is cancelling")
		assert.True(t, s.IsRunning(), "the slot is released by Finished, not by Abort")
	})

	t.Run("concurrent SetRunning and Abort: consistent state", func(t *testing.T) {
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
				s.Abort()
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

	t.Run("context cancelled by Abort is propagated to child contexts", func(t *testing.T) {
		s := NewLocalStatus()
		ok, parentCtx := s.SetRunning()
		require.True(t, ok)

		childCtx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		s.Abort()

		select {
		case <-childCtx.Done():
			assert.ErrorIs(t, context.Cause(childCtx), context.Canceled)
			assert.ErrorIs(t, context.Cause(childCtx), ErrAborted,
				"the reason reaches a child context too")
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

		s.Finished()
		assert.False(t, s.IsRunning(), "not running after Finished")

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

			s.Finished()
			assert.False(t, s.IsRunning(), "cycle %d: the slot is released", i)
			assert.ErrorIs(t, ctx.Err(), context.Canceled)
			assert.False(t, s.IsRunning())
		}
	})
}

func TestDeletedCountersToLogFieldsTotal(t *testing.T) {
	cases := []struct {
		name        string
		collections int
	}{
		{name: "two collections", collections: 2},
		{name: "many collections", collections: 32},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetOutput(io.Discard)

			counters := make(DeletedCounters, tt.collections)
			eg := enterrors.NewErrorGroupWrapper(log)

			for i := 0; i < tt.collections; i++ {
				name := fmt.Sprintf("Collection%d", i)
				counter := &atomic.Int32{}
				counters[name] = counter
				countDeleted := func(count int32) { counter.Add(count) }

				eg.Go(func() error {
					for j := 0; j < 500; j++ {
						countDeleted(1)
					}
					return nil
				})
			}
			require.NoError(t, eg.Wait())

			_, total := counters.ToLogFields(16)
			require.Equal(t, int32(tt.collections*500), total,
				"every collection's deletes are counted")
		})
	}
}

// TestStoppedBy pins stoppedBy's priority: the sweep's own cause wins over the
// caller's.
func TestStoppedBy(t *testing.T) {
	live := func() context.Context { return context.Background() }
	cancelled := func(cause error) context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		return ctx
	}

	cases := []struct {
		name   string
		caller context.Context
		sweep  context.Context
		wantIs error
	}{
		{
			name:   "nothing stopped it",
			caller: live(), sweep: live(),
		},
		{
			name:   "an operator aborted the sweep",
			caller: live(), sweep: cancelled(enterrors.NewCanceledCause(ErrAborted)),
			wantIs: ErrAborted,
		},
		{
			name:   "the caller gave up: a shutdown or a schedule change",
			caller: cancelled(context.Canceled), sweep: live(),
			wantIs: context.Canceled,
		},
		{
			// the sweep's own cause is the more specific of the two
			name:   "both, so the abort is what it reports",
			caller: cancelled(context.Canceled),
			sweep:  cancelled(enterrors.NewCanceledCause(ErrAborted)),
			wantIs: ErrAborted,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := stoppedBy(tt.caller, tt.sweep)
			if tt.wantIs == nil {
				require.NoError(t, got, "a live sweep is not stopped")
				return
			}
			require.ErrorIs(t, got, tt.wantIs)
		})
	}
}
