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
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// FlushAndSwitch has more than one caller — the flush cycle callback plus
// control-plane paths like backup and force-flush — and nothing stopped two of
// them from running at once. b.flushing is written under flushLock but read
// unlocked for the whole flush, so an overlapping caller could rebind it to its
// own memtable and strand the first caller's, dropping those keys out of every
// read path until the bucket is reopened from the WAL.
func TestBucketConcurrentFlushAndSwitch(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	const (
		flushers = 4
		rounds   = 25
		perRound = 20
	)

	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%05d", i)) }
	value := func(i int) []byte { return []byte(fmt.Sprintf("value-%05d", i)) }

	writesDone := make(chan struct{})
	var wg sync.WaitGroup

	// Writers and flushers overlap so a switch lands mid-write, the arrangement
	// the flush callback and a control-plane caller produce in production.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writesDone)
		for i := 0; i < rounds*perRound; i++ {
			if err := b.Put(key(i), value(i)); err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
		}
	}()

	errs := make(chan error, flushers*rounds)
	for range flushers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-writesDone:
					return
				default:
				}
				if err := b.FlushAndSwitch(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.NoError(t, b.FlushAndSwitch())

	// Every key must still be reachable. A stranded memtable is invisible to
	// Get even though its WAL survives on disk.
	for i := 0; i < rounds*perRound; i++ {
		got, err := b.Get(key(i))
		require.NoError(t, err, "get %s", key(i))
		require.Equal(t, value(i), got, "key %s lost by a concurrent flush", key(i))
	}
}
