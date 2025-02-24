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

package semaphore

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSemaphore(t *testing.T) {
	sem := NewSemaphore(10)
	wg := sync.WaitGroup{}

	// we are starting 20 goroutines, but at maximum 10 can run concurrently
	// => total runtime should be at least 20ms
	start := time.Now()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			sem.Acquire()
			time.Sleep(time.Millisecond * 10)
			wg.Done()
			sem.Release()
		}()
	}
	wg.Wait()

	require.Greater(t, time.Since(start), time.Millisecond*20)
}
