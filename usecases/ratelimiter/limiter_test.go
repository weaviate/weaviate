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

package ratelimiter

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimiter(t *testing.T) {
	l := New(3)

	// add 3 requests, should all work
	assert.True(t, l.TryInc())
	assert.True(t, l.TryInc())
	assert.True(t, l.TryInc())

	// try to add one more, should fail
	assert.False(t, l.TryInc())

	// decrease and try again
	l.Dec()
	l.Dec()
	assert.True(t, l.TryInc())
	assert.True(t, l.TryInc())
	assert.False(t, l.TryInc())
}

func TestLimiterConcurrently(t *testing.T) {
	var count int
	lock := &sync.Mutex{}

	l := New(30)

	request := func() {
		lock.Lock()
		count++
		if count > 30 {
			t.Fail()
		}
		lock.Unlock()

		time.Sleep(30 * time.Millisecond)

		lock.Lock()
		count--
		lock.Unlock()
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			if l.TryInc() {
				request()
				l.Dec()
			}
		}()
	}

	wg.Wait()
}

func TestLimiterUnlimited(t *testing.T) {
	l := New(-1)

	for i := 0; i < 1000; i++ {
		assert.True(t, l.TryInc())
	}

	for i := 0; i < 1000; i++ {
		l.Dec()
	}

	assert.True(t, l.TryInc())
}

func TestLimiterCantGoNegative(t *testing.T) {
	l := New(3)

	for i := 0; i < 10; i++ {
		l.Dec()
	}

	for i := 0; i < 3; i++ {
		assert.True(t, l.TryInc())
	}
	assert.False(t, l.TryInc())
}

func BenchmarkLimiter(b *testing.B) {
	l := New(-1)
	for i := 0; i < b.N; i++ {
		l.TryInc()
		l.Dec()
	}
}
