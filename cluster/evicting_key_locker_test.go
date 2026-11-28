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

package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A caller left on an entry dropped under contention deadlocks the rest or, under -race, races on the counter.
func TestEvictingKeyLocker_ContendedKey(t *testing.T) {
	const (
		callers    = 32
		iterations = 500
		key        = "k"
	)
	l := newEvictingKeyLocker()
	counter := 0

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				l.Lock(key)
				counter++
				l.Unlock(key)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a caller never got its lock")
	}

	require.Equal(t, callers*iterations, counter)
	require.Zero(t, lockEntries(l))
}
