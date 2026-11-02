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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// s5cmd's logger is a process global, so this runs in its own process: any
// earlier command in the same binary would have set it.
func TestHasDataNeedsNoEarlierCommand(t *testing.T) {
	const child = "OFFLOAD_HAS_DATA_CHILD"
	if os.Getenv(child) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHasDataNeedsNoEarlierCommand$")
		cmd.Env = append(os.Environ(), child+"=1",
			"AWS_ACCESS_KEY_ID=key", "AWS_SECRET_ACCESS_KEY=secret", "AWS_REGION=", "AWS_DEFAULT_REGION=")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			// s5cmd logs a failed region lookup, then carries on.
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<IsTruncated>false</IsTruncated>
			<Contents><Key>class/tenant/node/segment</Key><Size>1</Size></Contents>
		</ListBucketResult>`))
	}))
	defer srv.Close()

	m := &Module{Endpoint: srv.URL, Bucket: "bucket", timeout: 10 * time.Second, workersCount: 1}
	hasData, err := m.HasData(t.Context(), "Class", "tenant", "node")
	require.NoError(t, err)
	require.True(t, hasData)
}

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

func TestUploadArgs_SkipsScratchFiles(t *testing.T) {
	// Constructed directly: New() registers Prometheus collectors, and the
	// suite already has one module registered.
	m := &Module{
		Endpoint:    "http://127.0.0.1:1",
		Bucket:      "weaviate-offload-test",
		Concurrency: 4,
	}

	args := m.uploadArgs("/var/lib/weaviate/testclass/tenant-0", "TestClass", "tenant-0", "node1")

	// Exact argv, because position matters: s5cmd stops reading flags at the
	// first positional, so an --exclude placed after the source wildcard
	// becomes a third positional and cp fails with "expected source and
	// destination arguments".
	require.Equal(t, []string{
		"--endpoint-url=http://127.0.0.1:1",
		"cp",
		"--concurrency=4",
		"--exclude=*.tmp",
		"--exclude=hashtree_uuid/*",
		"/var/lib/weaviate/testclass/tenant-0/*",
		"s3://weaviate-offload-test/testclass/tenant-0/node1/",
	}, args)
}

func TestDeleteClassArgsKeepsRetainedCopies(t *testing.T) {
	m := &Module{Endpoint: "http://127.0.0.1:1", Bucket: "weaviate-offload-test"}

	args := m.deleteClassArgs("TestClass", []string{"tenant-0/7", "tenant-1/12"})

	require.Equal(t, []string{
		"--endpoint-url=http://127.0.0.1:1",
		"rm",
		"--exclude=.retained/*",
		"--exclude=tenant-0/7/*",
		"--exclude=tenant-1/12/*",
		"s3://weaviate-offload-test/testclass/*",
	}, args)
}

func TestRetainWritesAMarkerPerBackup(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	var mu sync.Mutex
	var puts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			mu.Lock()
			puts = append(puts, r.URL.Path)
			mu.Unlock()
		}
	}))
	defer srv.Close()

	m := &Module{Endpoint: srv.URL, Bucket: "bucket", timeout: 10 * time.Second, workersCount: 1}
	require.NoError(t, m.Retain(t.Context(), "Class", "tenant", "7", "backup-1"))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"/bucket/class/.retained/tenant/7/backup-1"}, puts)
}

func shardName(i int) string {
	return fmt.Sprintf("tenant-%d", i)
}
