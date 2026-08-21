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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestLazyLoadShardGetStatusDoesNotBlockOnLoadMutex(t *testing.T) {
	l := &LazyLoadShard{}
	l.mutex.Lock()
	defer l.mutex.Unlock()

	done := make(chan storagestate.Status, 1)
	go func() { done <- l.GetStatus() }()

	select {
	case status := <-done:
		assert.Equal(t, storagestate.StatusLazyLoading, status)
	case <-time.After(2 * time.Second):
		t.Fatal("GetStatus blocked on the load mutex")
	}
}

func TestLazyLoadShardLoadAsyncSkips(t *testing.T) {
	t.Run("while a load holds the mutex", func(t *testing.T) {
		l := &LazyLoadShard{}
		l.mutex.Lock()
		defer l.mutex.Unlock()
		l.LoadAsync()
	})

	t.Run("when already loaded", func(t *testing.T) {
		l := &LazyLoadShard{}
		l.loaded.Store(true)
		l.LoadAsync()
	})
}
