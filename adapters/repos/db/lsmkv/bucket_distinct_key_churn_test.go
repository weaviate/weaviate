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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// walkUnderChurn calls walkOnce in a loop while a writer rolls segments and a
// compactor swaps them underneath it. write receives a counter it can turn
// into whatever keys and doc ids the walk expects. It fails the test unless
// the walks actually raced a flush and a compaction, so a walk that stopped
// seeing churn cannot pass by doing nothing.
func walkUnderChurn(t *testing.T, b *Bucket, write func(n int) error, walkOnce func()) {
	t.Helper()

	stop := make(chan struct{})
	errs := make(chan error, 2)
	var flushes, compactions atomic.Int64
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}

			err := write(n)
			if err == nil && n%64 == 63 {
				if err = b.FlushAndSwitch(); err == nil {
					flushes.Add(1)
				}
			}
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			compacted, err := b.disk.compactOnce(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if compacted {
				compactions.Add(1)
			} else {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	walks := 0
	for ; walks < 500 && time.Now().Before(deadline); walks++ {
		walkOnce()
	}

	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Greater(t, walks, 20, "too few walks to have raced the writer")
	require.NotZero(t, flushes.Load(), "no segment was rolled, the walks raced nothing")
	require.NotZero(t, compactions.Load(), "no compaction ran, the walks raced nothing")
}
