//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCycleCombineCallbackCtrl_Unregister(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("unregisters both", func(t *testing.T) {
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrl1, ctrl2)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		err := combinedCtrl.Unregister(ctx)
		require.Nil(t, err)

		assert.False(t, combinedCtrl.IsActive())
		assert.False(t, ctrl1.IsActive())
		assert.False(t, ctrl2.IsActive())
	})

	t.Run("does not unregister on expired context", func(t *testing.T) {
		expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
		defer cancel()

		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrl1, ctrl2)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		err := combinedCtrl.Unregister(expiredCtx)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "unregistering callback 'c1' of 'id' failed: context deadline exceeded")
		assert.Contains(t, err.Error(), "unregistering callback 'c2' of 'id' failed: context deadline exceeded")

		assert.True(t, combinedCtrl.IsActive())
		assert.True(t, ctrl1.IsActive())
		assert.True(t, ctrl2.IsActive())
	})

	t.Run("fails unregistering one", func(t *testing.T) {
		callbackShort := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callbackLong := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(500 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrlShort := callbacks.Register("short", callbackShort)
		ctrlLong := callbacks.Register("long", callbackLong)
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrlShort, ctrlLong)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		// wait long enough to call Unregister while 2nd callback is still processed.
		// set timeout short enough to expire before 2nd callback finishes
		time.Sleep(300 * time.Millisecond)
		expirableCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err := combinedCtrl.Unregister(expirableCtx)
		require.NotNil(t, err)
		assert.EqualError(t, err, "unregistering callback 'long' of 'id' failed: context deadline exceeded")

		assert.False(t, combinedCtrl.IsActive())
		assert.False(t, ctrlShort.IsActive())
		assert.True(t, ctrlLong.IsActive())
	})
}

func TestCycleCombineCallbackCtrl_Deactivate(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("deactivates both", func(t *testing.T) {
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrl1, ctrl2)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		err := combinedCtrl.Deactivate(ctx)
		require.Nil(t, err)

		assert.False(t, combinedCtrl.IsActive())
		assert.False(t, ctrl1.IsActive())
		assert.False(t, ctrl2.IsActive())
	})

	t.Run("does not deactivate on expired context", func(t *testing.T) {
		expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
		defer cancel()

		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrl1, ctrl2)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		err := combinedCtrl.Deactivate(expiredCtx)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "deactivating callback 'c1' of 'id' failed: context deadline exceeded")
		assert.Contains(t, err.Error(), "deactivating callback 'c1' of 'id' failed: context deadline exceeded")

		assert.True(t, combinedCtrl.IsActive())
		assert.True(t, ctrl1.IsActive())
		assert.True(t, ctrl2.IsActive())
	})

	t.Run("fails deactivating one, activates other again", func(t *testing.T) {
		callbackShort := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callbackLong := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(500 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrlShort := callbacks.Register("short", callbackShort)
		ctrlLong := callbacks.Register("long", callbackLong)
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrlShort, ctrlLong)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		// wait long enough to call Deactivate while 2nd callback is still processed.
		// set timeout short enough to expire before 2nd callback finishes
		time.Sleep(300 * time.Millisecond)
		expirableCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err := combinedCtrl.Deactivate(expirableCtx)
		require.NotNil(t, err)
		assert.EqualError(t, err, "deactivating callback 'long' of 'id' failed: context deadline exceeded")

		assert.True(t, combinedCtrl.IsActive())
		assert.True(t, ctrlShort.IsActive())
		assert.True(t, ctrlLong.IsActive())
	})
}

func TestCycleCombineCallbackCtrl_Activate(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("activates both", func(t *testing.T) {
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1, AsInactive())
		ctrl2 := callbacks.Register("c2", callback2, AsInactive())
		combinedCtrl := NewCombinedCallbackCtrl(2, ctrl1, ctrl2)

		cycle := NewManager(NewFixedTicker(100*time.Millisecond), callbacks.CycleCallback)
		cycle.Start()
		defer cycle.StopAndWait(ctx)

		assert.False(t, combinedCtrl.IsActive())
		assert.False(t, ctrl1.IsActive())
		assert.False(t, ctrl2.IsActive())

		err := combinedCtrl.Activate()
		require.Nil(t, err)

		assert.True(t, combinedCtrl.IsActive())
		assert.True(t, ctrl1.IsActive())
		assert.True(t, ctrl2.IsActive())
	})
}
