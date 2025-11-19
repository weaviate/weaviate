//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sync

import (
	"context"
	"testing"
	"time"
)

const id = "id"

func BenchmarkKeyLocker(b *testing.B) {
	kl := NewKeyLocker()

	for i := 0; i < b.N; i++ {
		kl.Lock(id)
		kl.Unlock(id)
	}
}

func BenchmarkKeyLockerContextNormalLock(b *testing.B) {
	kl := NewKeyLockerContext()

	for i := 0; i < b.N; i++ {
		kl.Lock(id)
		kl.Unlock(id)
	}
}

func BenchmarkKeyLockerContext(b *testing.B) {
	kl := NewKeyLockerContext()

	for i := 0; i < b.N; i++ {
		if err := kl.LockWithContext(id, b.Context()); err == nil {
			kl.Unlock(id)
		}
	}
}

func BenchmarkConcurrentKeyLocker(b *testing.B) {
	kl := NewKeyLocker()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kl.Lock(id)
			kl.Unlock(id)
		}
	})
}

func BenchmarkConcurrentKeyLockerContext(b *testing.B) {
	kl := NewKeyLockerContext()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := kl.LockWithContext(id, b.Context()); err == nil {
				kl.Unlock(id)
			}
		}
	})
}

func BenchmarkKeyLockerUnlockAll(b *testing.B) {
	kl := NewKeyLocker()

	numToUnlock := b.N
	locksAcquired := make(chan interface{}, numToUnlock)
	for j := 0; j < numToUnlock; j++ {
		go func() {
			kl.Lock(id)
			locksAcquired <- struct{}{}
		}()
	}

	for i := 0; i < numToUnlock; i++ {
		<-locksAcquired
		kl.Unlock(id)
	}
	close(locksAcquired)
}

func BenchmarkKeyLockerContextUnlockAll(b *testing.B) {
	kl := NewKeyLockerContext()

	ctx, cancel := context.WithTimeout(b.Context(), 10*time.Second)
	defer cancel()

	numToUnlock := b.N
	locksAcquired := make(chan interface{}, numToUnlock)
	for j := 0; j < numToUnlock; j++ {
		go func() {
			if err := kl.LockWithContext(id, ctx); err == nil {
				locksAcquired <- struct{}{}
			}
		}()
	}

	for i := 0; i < numToUnlock; i++ {
		select {
		case <-locksAcquired:
		case <-ctx.Done():
			break
		}
		kl.Unlock(id)
	}

	close(locksAcquired)
}
