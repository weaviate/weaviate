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

package file

import (
	"context"
	"os"
	"time"
)

// WaitForTestHold is a test-only hook. When the environment variable envName
// names a path, it blocks until a file exists at that path, so an acceptance
// test can hold an operation open and release it by creating the file. It
// returns nil at once when envName is unset, and ctx.Err() if ctx ends first.
func WaitForTestHold(ctx context.Context, envName string) error {
	path := os.Getenv(envName)
	if path == "" {
		return nil
	}

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
