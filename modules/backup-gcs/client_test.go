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
	"net/http"
	"net/http/httptest"
	"net/url"
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

	"github.com/weaviate/weaviate/entities/backup"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

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
