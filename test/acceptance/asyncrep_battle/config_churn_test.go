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

package asyncrep_battle

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/helper"
)

// TestBattleConfigChurn loops class async-config updates and runtime
// kill-switch toggles under write load. The 60s per-update watchdog turns an
// apply-lock/RAFT wedge into a fast, attributable failure; isolated in its own
// compose because a wedge poisons the cluster for anything after.
func TestBattleConfigChurn(t *testing.T) {
	p := battleProfile()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	compose := buildCompose(ctx, t, nil)
	defer func() {
		if t.Failed() {
			compose.DumpWeaviateLogs(ctx, os.Stdout, 400)
		}
		require.NoError(t, compose.Terminate(ctx))
	}()

	const class = "BattleS5"
	uri1 := compose.GetWeaviateNode(1).URI()
	helper.SetupClient(uri1)
	helper.CreateClass(t, battleClass(class, 3, false))
	seedSentinel(t, uri1, class)
	seedObjects(t, uri1, class, 100)

	lowRate := p
	lowRate.writerGoroutines = 2
	lowRate.opInterval = 100 * time.Millisecond
	lowRate.idSpace = 300
	w := newChurnWriter(class, lowRate, nodeURIs(compose))
	w.start()

	disabled := false
	for i := 1; i <= p.configChurnIters; i++ {
		cls := common.GetClass(t, uri1, class)
		cls.ReplicationConfig.AsyncConfig.Frequency = i64(int64(5000 + (i%2)*1000))
		cls.ReplicationConfig.AsyncConfig.PropagationDelay = i64(int64(1000 + (i%2)*1000))
		updateClassWithDeadline(t, uri1, i, 60*time.Second, func() {
			common.UpdateClass(t, uri1, cls)
		})

		if i%3 == 0 {
			disabled = !disabled
			writeAsyncReplicationOverride(ctx, t, compose, disabled)
			time.Sleep(2 * time.Second)
		}
	}

	writeAsyncReplicationOverride(ctx, t, compose, false)
	helper.SetupClient(uri1)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		n, err := shardsAsyncReplicationLen(t, class)
		require.NoError(ct, err)
		require.Greater(ct, n, 0, "async replication must be registered after the final enable")
	}, 60*time.Second, 2*time.Second)

	acked, errs := w.stop()
	t.Logf("writer finished: %d acked ops, %d errors", acked, errs)
	require.Greater(t, acked, int64(0))

	requireConverged(ctx, t, compose, class, lowRate.idSpace*2, p.convergeTimeout)
	for n := 1; n <= 3; n++ {
		text := scrapeMetrics(ctx, t, compose, n)
		require.Zero(t, sumMetric(text, "weaviate_async_replication_reconcile_failures_total"),
			"node %d recorded reconcile failures during config churn", n)
		require.Zero(t, sumMetric(text, "weaviate_async_replication_rebuild_failures_total"),
			"node %d recorded rebuild failures during config churn", n)
	}
	requireCleanLogs(ctx, t, compose)
}

// updateClassWithDeadline fails fast when a schema update hangs — the
// symptom of a config-apply deadlock wedging the RAFT FSM.
func updateClassWithDeadline(t *testing.T, uri string, iter int, deadline time.Duration, update func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		update()
	}()
	select {
	case <-done:
	case <-time.After(deadline):
		t.Fatalf("schema update wedged for %v at iteration %d against %s — apply-lock deadlock suspected", deadline, iter, uri)
	}
}
