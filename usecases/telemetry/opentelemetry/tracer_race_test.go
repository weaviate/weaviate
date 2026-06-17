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

package opentelemetry

import (
	"context"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestGlobalProvider_ConcurrentAccess pins the globalProvider data race: it must
// be safe to Init the provider concurrently with hot-path readers. Before the
// atomic.Pointer conversion this failed under `go test -race`; with it, the
// lockless Load/Store reads are clean.
func TestGlobalProvider_ConcurrentAccess(t *testing.T) {
	logger, _ := test.NewNullLogger()
	t.Cleanup(func() { globalProvider.Store(nil) })

	var wg sync.WaitGroup
	start := make(chan struct{})

	const readers = 8
	for i := 0; i < readers; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				_ = IsEnabled()
				_ = GetTracer()
			}
		}, logger)
	}

	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		<-start
		for j := 0; j < 50; j++ {
			// OTEL is disabled by default in tests, so this builds and stores a
			// no-op provider repeatedly — exactly the startup write to race against.
			_ = Init(logger)
		}
	}, logger)

	close(start)
	wg.Wait()

	_ = Shutdown(context.Background())
}
