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

package modsloads3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestUpload_ConcurrentNoFlagRedefinedPanic(t *testing.T) {
	dataDir := t.TempDir()
	className := "TestClass"
	const tenants = 32

	for i := 0; i < tenants; i++ {
		shardDir := filepath.Join(dataDir, strings.ToLower(className), shardName(i))
		require.NoError(t, os.MkdirAll(shardDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(shardDir, "data.bin"), []byte("payload"), 0o644))
	}

	// Route the endpoint via the environment variable so every concurrent upload
	// uses the same unreachable endpoint without relying on command-line flags.
	const unreachable = "http://127.0.0.1:1"
	t.Setenv("OFFLOAD_S3_ENDPOINT", unreachable)

	logger, _ := test.NewNullLogger()
	m := New()
	m.logger = logger
	m.DataPath = dataDir
	m.Endpoint = unreachable
	m.Bucket = "weaviate-offload-test"
	m.timeout = 2 * time.Second

	var wg sync.WaitGroup
	panics := make(chan any, tenants)
	for i := 0; i < tenants; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics <- r
				}
			}()
			// We expect an error (unreachable endpoint), but never a panic.
			_ = m.Upload(context.Background(), className, shardName(i), "test-node")
		}()
	}
	wg.Wait()
	close(panics)

	for r := range panics {
		t.Errorf("Upload panicked: %v", r)
	}
}

func shardName(i int) string {
	return fmt.Sprintf("tenant-%d", i)
}
