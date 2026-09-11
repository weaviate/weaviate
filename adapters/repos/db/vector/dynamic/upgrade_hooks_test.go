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

package dynamic

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDynamicUpgradeHooks pins the SetUpgradeHooks contract the shard's
// docID-reuse pause depends on: start fires synchronously as soon as the
// attempt is claimed (before any copying), done fires when the attempt
// finishes — successful or not — and neither fires for callers that lose the
// claim.
func TestDynamicUpgradeHooks(t *testing.T) {
	t.Run("bracket_successful_upgrade", func(t *testing.T) {
		dyn := newUpgradeRetryTestDynamic(t, 10)

		var starts, dones atomic.Int32
		doneCh := make(chan struct{})
		dyn.SetUpgradeHooks(
			func() { starts.Add(1) },
			func() { dones.Add(1); close(doneCh) },
		)

		upgradeStarted := make(chan struct{})
		release := make(chan struct{})
		realUpgrade := dyn.upgradeFn
		dyn.upgradeFn = func() error {
			close(upgradeStarted)
			<-release
			return realUpgrade()
		}

		require.NoError(t, dyn.Upgrade(func() {}))
		// start must have fired synchronously at claim time, before the
		// rebuild ran at all
		require.Equal(t, int32(1), starts.Load(), "start hook must fire at claim time")
		require.Equal(t, int32(0), dones.Load(), "done hook must not fire before the attempt ends")

		<-upgradeStarted
		close(release)
		select {
		case <-doneCh:
		case <-time.After(5 * time.Second):
			t.Fatal("done hook was not invoked after the upgrade finished")
		}
		require.True(t, dyn.IsUpgraded())
		require.Equal(t, int32(1), starts.Load())
	})

	t.Run("bracket_failed_upgrade", func(t *testing.T) {
		dyn := newUpgradeRetryTestDynamic(t, 10)

		var starts, dones atomic.Int32
		doneCh := make(chan struct{})
		dyn.SetUpgradeHooks(
			func() { starts.Add(1) },
			func() { dones.Add(1); close(doneCh) },
		)
		dyn.upgradeFn = func() error { return errors.New("injected failure") }

		require.NoError(t, dyn.Upgrade(func() {}))
		select {
		case <-doneCh:
		case <-time.After(5 * time.Second):
			t.Fatal("done hook must fire even when the upgrade fails")
		}
		require.Equal(t, int32(1), starts.Load())
		require.Equal(t, int32(1), dones.Load(),
			"a failed attempt must still release the reuse pause")
		require.False(t, dyn.IsUpgraded())
	})

	t.Run("losing_claimant_fires_no_hooks", func(t *testing.T) {
		dyn := newUpgradeRetryTestDynamic(t, 10)

		var starts atomic.Int32
		dyn.SetUpgradeHooks(func() { starts.Add(1) }, nil)

		release := make(chan struct{})
		started := make(chan struct{})
		dyn.upgradeFn = func() error {
			close(started)
			<-release
			return errors.New("stop here")
		}

		require.NoError(t, dyn.Upgrade(func() {}))
		<-started
		require.Equal(t, int32(1), starts.Load())

		// a second caller loses the claim: its Upgrade resolves its callback
		// without firing the hooks again
		cb := make(chan struct{})
		require.NoError(t, dyn.Upgrade(func() { close(cb) }))
		<-cb
		require.Equal(t, int32(1), starts.Load(),
			"a losing claimant must not fire the start hook again")

		close(release)
	})
}
