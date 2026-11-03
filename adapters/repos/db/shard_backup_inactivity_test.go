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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A halt that returns successfully is itself liveness. Without pushing the
// deadline forward, an overlapping consumer inherits a deadline the first
// consumer's seal steps already spent, and the watchdog's force-resume drops
// the halt for every holder rather than only the one that timed out.
func TestShard_HaltForTransferExtendsInactivityDeadline(t *testing.T) {
	ctx := testCtx()
	shd, idx := testShard(t, ctx, "TestClass")
	t.Cleanup(func() {
		_ = idx.drop()
		_ = os.RemoveAll(idx.Config.RootPath)
	})
	s := shd.(*Shard)

	require.NoError(t, s.HaltForTransfer(ctx, false, time.Hour))

	// Stands in for a halt whose preparation outlasted the remaining budget.
	s.haltForTransferMux.Lock()
	s.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
	s.haltForTransferMux.Unlock()

	require.NoError(t, s.HaltForTransfer(ctx, false, time.Hour))

	s.haltForTransferMux.Lock()
	deadline := s.haltForTransferInactivityDeadline
	s.haltForTransferMux.Unlock()
	require.True(t, deadline.After(time.Now()),
		"an overlapping halt left the deadline in the past — the monitor will force-resume a shard two consumers still hold")

	for s.haltForTransferCount.Load() > 0 {
		require.NoError(t, s.resumeMaintenanceCycles(ctx))
	}
}
