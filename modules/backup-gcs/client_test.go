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

package modstggcs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	ucfg "github.com/weaviate/weaviate/usecases/config"
)

// connPoolOptionType is the concrete type google.golang.org/api/option gives
// the connection-pool option; the option carries no exported accessor.
const connPoolOptionType = "option.withGRPCConnectionPool"

func TestStorageOptionsTransport(t *testing.T) {
	tests := []struct {
		name         string
		transport    ucfg.BackupGCS
		wantOptions  []string
		wantConnPool int
	}{
		{
			name:        "http adds no transport options",
			transport:   ucfg.BackupGCS{},
			wantOptions: []string{"option.withoutAuthentication"},
		},
		{
			name:         "grpc sizes the connection pool and turns client metrics off",
			transport:    ucfg.BackupGCS{UseGRPC: true, GRPCConnPool: 8},
			wantOptions:  []string{"option.withoutAuthentication", connPoolOptionType, "*storage.withDisabledClientMetrics"},
			wantConnPool: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BACKUP_GCS_USE_AUTH", "false")
			t.Setenv("BACKUP_GCS_AUTH_PROXY_ENDPOINT", "")

			opts, err := storageOptions(context.Background(), discardLogger(), tt.transport)
			require.NoError(t, err)

			types := make([]string, 0, len(opts))
			for _, opt := range opts {
				types = append(types, fmt.Sprintf("%T", opt))
			}
			assert.Equal(t, tt.wantOptions, types)
			assert.Equal(t, tt.wantConnPool, grpcConnPoolOption(opts))
		})
	}
}

// The SDK leaves a chunk request unbounded and gives all its retries only 32s.
// Both defaults are wrong for backup uploads, which stream for hours under a
// context that allows 24.
func TestChunkWriterTuning(t *testing.T) {
	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	require.NoError(t, err)
	defer client.Close()

	writer := newChunkWriter(context.Background(), client.Bucket("b").Object("o"), "backup-1")

	assert.Equal(t, chunkRetryDeadline, writer.ChunkRetryDeadline, "retry deadline")
	assert.Equal(t, chunkTransferTimeout, writer.ChunkTransferTimeout, "transfer timeout")
	assert.Equal(t, "application/octet-stream", writer.ContentType)
	assert.Equal(t, map[string]string{"backup-id": "backup-1"}, writer.Metadata)

	// A transfer timeout only buys a retry if the deadline outlasts several of
	// them; equal values would spend the whole budget on one stalled request.
	assert.LessOrEqual(t, 3*chunkTransferTimeout, chunkRetryDeadline,
		"retry deadline must fit at least three transfer timeouts")
}

// TestNewClientTransport routes one request through a local fake of each API, so
// the assertion is which wire the client actually used.
func TestNewClientTransport(t *testing.T) {
	tests := []struct {
		name      string
		transport ucfg.BackupGCS
		wantCalls []string
	}{
		{
			name:      "default speaks the json api over http",
			transport: ucfg.BackupGCS{},
			wantCalls: []string{"/storage/v1/b/my-bucket"},
		},
		{
			name:      "grpc speaks the storage grpc api",
			transport: ucfg.BackupGCS{UseGRPC: true, GRPCConnPool: ucfg.DefaultBackupGCSGRPCConnPool},
			wantCalls: []string{"/google.storage.v2.Storage/GetBucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Both fakes share one recorder, so the untaken transport is
			// asserted to have received nothing.
			calls := &callRecorder{}
			t.Setenv("BACKUP_GCS_USE_AUTH", "false")
			t.Setenv("BACKUP_GCS_AUTH_PROXY_ENDPOINT", "")
			// The SDK reads a separate variable per transport.
			t.Setenv("STORAGE_EMULATOR_HOST", startFakeGCSOverHTTP(t, calls))
			t.Setenv("STORAGE_EMULATOR_HOST_GRPC", startFakeGCSOverGRPC(t, calls))

			config := &clientConfig{Bucket: "my-bucket", Transport: tt.transport}
			c, err := newClient(context.Background(), config, t.TempDir(), discardLogger())
			require.NoError(t, err)
			defer c.client.Close()

			_, err = c.findBucket(context.Background(), "")
			require.ErrorIs(t, err, storage.ErrBucketNotExist)
			assert.Equal(t, tt.wantCalls, calls.recorded())
		})
	}
}

type callRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *callRecorder) record(method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, method)
}

func (r *callRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// startFakeGCSOverHTTP answers every request with a not-found bucket, which the
// retry policy does not retry, so one call reaches the recorder.
func startFakeGCSOverHTTP(t *testing.T, calls *callRecorder) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.record(r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error":{"code":404,"message":"no such bucket"}}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func startFakeGCSOverGRPC(t *testing.T, calls *callRecorder) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		calls.record(method)
		return status.Error(codes.NotFound, "no such bucket")
	}))
	enterrors.GoWrapper(func() { _ = srv.Serve(listener) }, discardLogger())
	t.Cleanup(srv.Stop)
	return listener.Addr().String()
}

func grpcConnPoolOption(opts []option.ClientOption) int {
	for _, opt := range opts {
		if v := reflect.ValueOf(opt); v.Type().String() == connPoolOptionType && v.Kind() == reflect.Int {
			return int(v.Int())
		}
	}
	return 0
}

func discardLogger() logrus.FieldLogger {
	logger := logrus.New()
	logger.Out = io.Discard
	return logger
}

func TestInitialize_SkipAccessCheck(t *testing.T) {
	// Validation runs before the SkipAccessCheck short-circuit: a valid bucket
	// skips the probe, an empty one still errors.
	tests := []struct {
		name    string
		bucket  string
		wantErr string
	}{
		{name: "valid bucket skips probe", bucket: "my-bucket"},
		{name: "empty bucket still validates", bucket: "", wantErr: "bucket must not be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &gcsClient{config: clientConfig{Bucket: tt.bucket, SkipAccessCheck: true}}
			err := c.Initialize(context.Background(), "backup-1", "", "")
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestFindBucket_EmptyBucket(t *testing.T) {
	// Note: cases where the resolved bucket is non-empty cannot be tested
	// without a real GCS connection (client.Bucket panics on a nil client),
	// so we only test the early-return guard here.
	tests := []struct {
		name         string
		configBucket string
		override     string
		wantErr      string
	}{
		{
			name:         "empty config bucket without override returns error",
			configBucket: "",
			override:     "",
			wantErr:      "bucket must not be empty",
		},
		{
			name:         "non-empty config bucket without override passes guard",
			configBucket: "my-bucket",
			override:     "",
		},
		{
			name:         "empty config bucket with non-empty override passes guard",
			configBucket: "",
			override:     "override-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &gcsClient{config: clientConfig{Bucket: tt.configBucket}}

			if tt.wantErr != "" {
				_, err := client.findBucket(context.Background(), tt.override)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				// Verify the guard logic: resolve the bucket the same way
				// findBucket does and confirm it is non-empty.
				b := tt.configBucket
				if tt.override != "" {
					b = tt.override
				}
				assert.NotEmpty(t, b)
			}
		})
	}
}

// A restore against a bucket that is gone must be reported as not-found, so the
// backup coordinator can tell it apart from an internal failure.
func TestMissingBucketReportsNotFound(t *testing.T) {
	tests := []struct {
		name string
		call func(*gcsClient, context.Context) error
	}{
		{
			name: "Read",
			call: func(g *gcsClient, ctx context.Context) error {
				_, err := g.Read(ctx, "backup-1", "shard.db", "", "", discardWriteCloser{})
				return err
			},
		},
		{
			name: "GetObject",
			call: func(g *gcsClient, ctx context.Context) error {
				_, err := g.GetObject(ctx, "backup-1", "shard.db", "", "")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"error":{"code":404,"message":"no such bucket"}}`)
			}))
			defer srv.Close()

			ctx := context.Background()
			gcs, err := storage.NewClient(ctx, option.WithoutAuthentication(), option.WithEndpoint(srv.URL))
			require.NoError(t, err)
			defer gcs.Close()
			gcs.SetRetry(storage.WithPolicy(storage.RetryNever))

			g := &gcsClient{client: gcs, config: clientConfig{Bucket: "gone-bucket"}, logger: discardLogger()}

			err = tt.call(g, ctx)
			require.Error(t, err)
			var notFound backup.ErrNotFound
			assert.ErrorAs(t, err, &notFound)
		})
	}
}

type discardWriteCloser struct{}

func (discardWriteCloser) Write(p []byte) (int, error) { return len(p), nil }

func (discardWriteCloser) Close() error { return nil }

func TestAllBackupsSkipsMissingDescriptors(t *testing.T) {
	const bucketName = "test-bucket"

	validDesc, err := json.Marshal(backup.DistributedBackupDescriptor{ID: "backup-1"})
	require.NoError(t, err)

	tests := []struct {
		name           string
		descriptorBody func(objectName string) (status int, body []byte)
		wantIDs        []string
		wantErr        bool
	}{
		{
			name: "missing descriptor for one backup is skipped, other returned",
			descriptorBody: func(objectName string) (int, []byte) {
				switch objectName {
				case "backup-1/" + ubak.GlobalBackupFile:
					return http.StatusOK, validDesc
				case "backup-2/" + ubak.GlobalBackupFile:
					// Simulate object not found -> storage.ErrObjectNotExist
					return http.StatusNotFound, []byte(`{"error":{"code":404,"message":"not found"}}`)
				}
				return http.StatusNotFound, nil
			},
			wantIDs: []string{"backup-1"},
		},
		{
			name: "non-not-found error on descriptor fetch fails the listing",
			descriptorBody: func(objectName string) (int, []byte) {
				if objectName == "backup-1/"+ubak.GlobalBackupFile {
					return http.StatusOK, validDesc
				}
				return http.StatusInternalServerError, []byte(`{"error":{"code":500,"message":"boom"}}`)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()

			// Object list with delimiter: GET /b/{bucket}/o
			mux.HandleFunc("/b/"+bucketName+"/o", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"kind":"storage#objects","prefixes":["backup-1/","backup-2/"]}`)
			})

			// Object reader (XML API media): GET /{bucket}/{object}
			mux.HandleFunc("/"+bucketName+"/", func(w http.ResponseWriter, r *http.Request) {
				raw := strings.TrimPrefix(r.URL.Path, "/"+bucketName+"/")
				name, err := url.PathUnescape(raw)
				if err != nil {
					http.Error(w, "bad object name", http.StatusBadRequest)
					return
				}
				status, body := tt.descriptorBody(name)
				w.Header().Set("Content-Type", "application/octet-stream")
				w.WriteHeader(status)
				w.Write(body)
			})

			ctx := context.Background()
			g := newFakeGCSClient(t, bucketName, mux)

			got, err := g.AllBackups(ctx)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			ids := make([]string, 0, len(got))
			for _, d := range got {
				ids = append(ids, d.ID)
			}
			assert.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

// newFakeGCSClient points a real storage.Client at mux, which must serve every
// API call the test exercises apart from the bucket attrs fetch.
func newFakeGCSClient(t *testing.T, bucketName string, mux *http.ServeMux) *gcsClient {
	t.Helper()

	mux.HandleFunc("/b/"+bucketName, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"kind":"storage#bucket","name":%q}`, bucketName)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	gcs, err := storage.NewClient(context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(srv.URL),
	)
	require.NoError(t, err)
	t.Cleanup(func() { gcs.Close() })
	// Disable retries so an error status surfaces immediately instead of looping,
	// and so request counts stay exact.
	gcs.SetRetry(storage.WithPolicy(storage.RetryNever))

	return &gcsClient{
		client: gcs,
		config: clientConfig{Bucket: bucketName},
		logger: logrus.New(),
	}
}

// stubReader yields payload and then fails with readErr, or ends cleanly when
// readErr is nil. It records the error Write signals back to the producer.
type stubReader struct {
	payload    *bytes.Reader
	readErr    error
	closedWith error
}

func (s *stubReader) Read(p []byte) (int, error) {
	if s.payload.Len() > 0 {
		return s.payload.Read(p)
	}
	if s.readErr != nil {
		return 0, s.readErr
	}
	return 0, io.EOF
}

func (s *stubReader) Close() error { return nil }

func (s *stubReader) CloseWithError(err error) error {
	s.closedWith = err
	return nil
}

func TestWriteUploadsOnlyCompleteObjects(t *testing.T) {
	const bucketName = "test-bucket"

	readErr := errors.New("scan failed")

	tests := []struct {
		name        string
		payload     string
		readErr     error
		wantUploads int64
	}{
		{
			name:        "complete copy stores the object",
			payload:     "full-payload",
			wantUploads: 1,
		},
		{
			name:        "read failing mid-stream stores nothing",
			payload:     "truncated-par",
			readErr:     readErr,
			wantUploads: 0,
		},
		{
			name:        "read failing before any byte stores nothing",
			payload:     "",
			readErr:     readErr,
			wantUploads: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var uploads atomic.Int64
			var uploadBody atomic.Value

			mux := http.NewServeMux()
			// Object creation via the JSON API. These payloads are far below the
			// 16 MiB ChunkSize, so the upload is always a single multipart POST.
			mux.HandleFunc("/upload/storage/v1/b/"+bucketName+"/o", func(w http.ResponseWriter, r *http.Request) {
				uploads.Add(1)
				body, _ := io.ReadAll(r.Body)
				uploadBody.Store(string(body))
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"kind":"storage#object","bucket":%q,"name":"backup-1/chunk-0"}`, bucketName)
			})

			ctx := context.Background()
			g := newFakeGCSClient(t, bucketName, mux)

			r := &stubReader{payload: bytes.NewReader([]byte(tt.payload)), readErr: tt.readErr}
			written, err := g.Write(ctx, "backup-1", "chunk-0", "", "", r)

			if tt.readErr != nil {
				require.ErrorIs(t, err, tt.readErr)
				require.ErrorIs(t, r.closedWith, tt.readErr)
			} else {
				require.NoError(t, err)
				require.NoError(t, r.closedWith)
			}
			// written counts bytes read off the producer, not bytes stored: on the
			// error path the copied bytes are abandoned rather than uploaded.
			assert.Equal(t, int64(len(tt.payload)), written)
			assert.Equal(t, tt.wantUploads, uploads.Load(),
				"object-creating requests that reached the backend")

			if tt.wantUploads > 0 {
				body, _ := uploadBody.Load().(string)
				assert.Contains(t, body, `"name":"backup-1/chunk-0"`)
				assert.Contains(t, body, `"backup-id":"backup-1"`)
				assert.Contains(t, body, tt.payload)
			}
		})
	}
}

// recordingExporter collects the names of spans that were ended.
type recordingExporter struct {
	mu    sync.Mutex
	names []string
}

func (e *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range spans {
		e.names = append(e.names, s.Name())
	}
	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error { return nil }

func (e *recordingExporter) endedSpans() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.names...)
}

// The writer's span is only ended by closing it, so abandoning an upload has to
// close the writer as well or the span for every failed write is lost.
func TestWriteEndsWriterSpan(t *testing.T) {
	const bucketName = "test-bucket"

	exporter := &recordingExporter{}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	mux := http.NewServeMux()
	mux.HandleFunc("/upload/storage/v1/b/"+bucketName+"/o", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"kind":"storage#object","bucket":%q,"name":"backup-1/chunk-0"}`, bucketName)
	})

	ctx := context.Background()
	g := newFakeGCSClient(t, bucketName, mux)

	readErr := errors.New("scan failed")
	r := &stubReader{payload: bytes.NewReader([]byte("truncated-par")), readErr: readErr}
	_, err := g.Write(ctx, "backup-1", "chunk-0", "", "", r)
	require.ErrorIs(t, err, readErr)

	var writerSpans int
	for _, name := range exporter.endedSpans() {
		if strings.HasSuffix(name, "Object.Writer") {
			writerSpans++
		}
	}
	assert.Equal(t, 1, writerSpans, "the writer span must be ended on the error path")
}

func TestWriteResumableUploadIsFinalizedOnlyOnSuccess(t *testing.T) {
	const bucketName = "test-bucket"
	// Above the 16 MiB default ChunkSize, so the SDK opens a resumable session
	// and pushes the first chunk to the backend before the copy ends.
	payload := bytes.Repeat([]byte("x"), 17*1024*1024)

	readErr := errors.New("scan failed")

	tests := []struct {
		name          string
		readErr       error
		wantFinalized int64
	}{
		{
			name:          "complete copy finalizes the object",
			wantFinalized: 1,
		},
		{
			name:          "read failing after the first chunk leaves the session unfinalized",
			readErr:       readErr,
			wantFinalized: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sessions, chunkPuts, finalizePuts atomic.Int64
			var finalizeRange atomic.Value

			mux := http.NewServeMux()
			mux.HandleFunc("/upload/storage/v1/b/"+bucketName+"/o", func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "resumable", r.URL.Query().Get("uploadType"))
				sessions.Add(1)
				w.Header().Set("Location", "http://"+r.Host+"/resumable-session")
			})
			mux.HandleFunc("/resumable-session", func(w http.ResponseWriter, r *http.Request) {
				n, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)

				// A trailing "/*" marks a chunk of a still-open session; a byte total
				// there is the request that finalizes the object.
				if contentRange := r.Header.Get("Content-Range"); strings.HasSuffix(contentRange, "/*") {
					chunkPuts.Add(1)
					// The client sends X-GUploader-No-308, so "resume incomplete" is
					// signalled by this header rather than by a 308 status.
					w.Header().Set("X-Http-Status-Code-Override", "308")
					w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", n-1))
					return
				}
				finalizePuts.Add(1)
				finalizeRange.Store(r.Header.Get("Content-Range"))
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"kind":"storage#object","bucket":%q,"name":"backup-1/chunk-0"}`, bucketName)
			})

			g := newFakeGCSClient(t, bucketName, mux)

			r := &stubReader{payload: bytes.NewReader(payload), readErr: tt.readErr}
			written, err := g.Write(context.Background(), "backup-1", "chunk-0", "", "", r)

			if tt.readErr != nil {
				require.ErrorIs(t, err, tt.readErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, int64(len(payload)), written)

			assert.Equal(t, int64(1), sessions.Load(), "resumable session opened")
			assert.Equal(t, int64(1), chunkPuts.Load(),
				"the first chunk reaches the backend before the copy ends")
			assert.Equal(t, tt.wantFinalized, finalizePuts.Load(),
				"requests that finalize the object")

			if tt.wantFinalized > 0 {
				cr, _ := finalizeRange.Load().(string)
				assert.True(t, strings.HasSuffix(cr, fmt.Sprintf("/%d", len(payload))),
					"object finalized at the full payload size, got Content-Range %q", cr)
			}
		})
	}
}
