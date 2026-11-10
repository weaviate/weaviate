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

package hnsw

import (
	"sync"
	"sync/atomic"
)

// retryOnce runs a function until it succeeds once, unlike sync.Once, which a
// failed attempt uses up.
type retryOnce struct {
	done atomic.Bool
	mu   sync.Mutex
}

func (o *retryOnce) Do(f func() error) error {
	if o.done.Load() {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.done.Load() {
		return nil
	}
	if err := f(); err != nil {
		return err
	}
	o.done.Store(true)
	return nil
}
