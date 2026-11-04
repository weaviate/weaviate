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
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Runs in a child process: s5cmd's logger is a process global any earlier command would have set.
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

// s5cmd reports a listing cut short as success, which would cache a bucket it never saw.
func TestVerifyBucketCutShortFails(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	t.Setenv("OFFLOAD_S3_ENDPOINT", srv.URL)

	m := &Module{Endpoint: srv.URL, Bucket: "bucket", timeout: time.Minute, workersCount: 1}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, m.VerifyBucket(ctx), context.DeadlineExceeded)
	require.False(t, m.BucketExists.Load())
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

// A late-applied class delete must not touch copies of a same-name class created after it.
func TestDeleteClassKeepsNewerAndRetainedCopies(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	var mu sync.Mutex
	var deleted []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Objects []struct{ Key string } `xml:"Object"`
			}
			require.NoError(t, xml.Unmarshal(body, &req))
			out := `<DeleteResult>`
			mu.Lock()
			for _, o := range req.Objects {
				deleted = append(deleted, o.Key)
				out += "<Deleted><Key>" + o.Key + "</Key></Deleted>"
			}
			mu.Unlock()
			_, _ = w.Write([]byte(out + `</DeleteResult>`))
			return
		}
		keys := []string{
			"class/t1/7/node1/segment", "class/t1/7/node2/segment",
			"class/t1/120/node1/segment",
			"class/t2/9/node1/segment",
			"class/t3/legacy-node/segment",
			"class/.retained/t2/9/backup-1",
		}
		if prefix := r.URL.Query().Get("prefix"); prefix == "class/.retained/" {
			keys = keys[len(keys)-1:]
		}
		out := `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`
		for _, key := range keys {
			out += "<Contents><Key>" + key + "</Key><Size>1</Size></Contents>"
		}
		_, _ = w.Write([]byte(out + `</ListBucketResult>`))
	}))
	defer srv.Close()

	logger, _ := test.NewNullLogger()
	m := &Module{
		Endpoint: srv.URL, Bucket: "bucket", timeout: 10 * time.Second, workersCount: 1, logger: logger,
		metrics: monitoring.NewTenantOffloadMetrics(monitoring.Config{MetricsNamespace: "test"}, prometheus.NewRegistry()),
	}
	require.NoError(t, m.DeleteClass(t.Context(), "Class", 100))

	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []string{"class/t1/7/node1/segment", "class/t1/7/node2/segment"}, deleted)
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

func TestReleaseDeletesOnlyTheBackupsMarkers(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	var mu sync.Mutex
	var prefixes []string
	var deleted []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Objects []struct{ Key string } `xml:"Object"`
			}
			require.NoError(t, xml.Unmarshal(body, &req))
			out := `<DeleteResult>`
			mu.Lock()
			for _, o := range req.Objects {
				deleted = append(deleted, o.Key)
				out += "<Deleted><Key>" + o.Key + "</Key></Deleted>"
			}
			mu.Unlock()
			_, _ = w.Write([]byte(out + `</DeleteResult>`))
			return
		}
		mu.Lock()
		prefixes = append(prefixes, r.URL.Query().Get("prefix"))
		mu.Unlock()
		_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<IsTruncated>false</IsTruncated>
			<Contents><Key>class/.retained/t1/7/backup-1</Key><Size>0</Size></Contents>
			<Contents><Key>class/.retained/t1/7/backup-2</Key><Size>0</Size></Contents>
			<Contents><Key>class/.retained/t2/9/backup-1</Key><Size>0</Size></Contents>
			<Contents><Key>class/.retained/t3/4/backup-10</Key><Size>0</Size></Contents>
		</ListBucketResult>`))
	}))
	defer srv.Close()

	m := &Module{Endpoint: srv.URL, Bucket: "bucket", timeout: 10 * time.Second, workersCount: 1}
	require.NoError(t, m.Release(t.Context(), "Class", "backup-1"))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"class/.retained/"}, prefixes)
	require.ElementsMatch(t, []string{"class/.retained/t1/7/backup-1", "class/.retained/t2/9/backup-1"}, deleted)
}

// A listing cut short must not pass as one that found no marker.
func TestReleaseCutShortFails(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	m := &Module{Endpoint: srv.URL, Bucket: "bucket", timeout: time.Minute, workersCount: 1}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	require.Error(t, m.Release(ctx, "Class", "backup-1"))
}

// s5cmd reports a cut-short transfer as success, passing a partial copy or an empty download.
func TestTransfersCutShortFail(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a cancelled upload can leave its request open
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	defer srv.Close()
	defer close(stop)
	t.Setenv("OFFLOAD_S3_ENDPOINT", srv.URL)

	dataPath := t.TempDir()
	shardDir := filepath.Join(dataPath, "class", "tenant")
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "segment"), []byte("payload"), 0o644))
	logger, _ := test.NewNullLogger()
	m := &Module{
		Endpoint: srv.URL, Bucket: "bucket", DataPath: dataPath, Concurrency: 1,
		timeout: time.Minute, workersCount: 1, logger: logger,
		metrics: monitoring.NewTenantOffloadMetrics(monitoring.Config{MetricsNamespace: "test"}, prometheus.NewRegistry()),
	}
	staging := filepath.Join(dataPath, "staging")

	tests := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{name: "upload", run: func(ctx context.Context) error { return m.Upload(ctx, "Class", "tenant", "7/node1") }},
		{name: "download", run: func(ctx context.Context) error { return m.DownloadToPath(ctx, "Class", "tenant", "7/node1", staging) }},
		{name: "delete", run: func(ctx context.Context) error { return m.Delete(ctx, "Class", "tenant", "7") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			require.Error(t, tt.run(ctx))
		})
	}
}

// Transition cleanup deletes every attempt ListCopies names, so it must name exactly those folders.
func TestListCopiesNamesTheAttempts(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query().Get("prefix")+"|"+r.URL.Query().Get("delimiter"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<IsTruncated>false</IsTruncated>
			<CommonPrefixes><Prefix>class/tenant/7/</Prefix></CommonPrefixes>
			<CommonPrefixes><Prefix>class/tenant/12/</Prefix></CommonPrefixes>
		</ListBucketResult>`))
	}))
	defer srv.Close()

	m := &Module{Endpoint: srv.URL, Bucket: "bucket", timeout: 10 * time.Second, workersCount: 1}
	copies, err := m.ListCopies(t.Context(), "Class", "tenant")

	require.NoError(t, err)
	require.ElementsMatch(t, []string{"7", "12"}, copies)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"class/tenant/|/"}, queries, "one level only, so a node's files are not listed")
}
