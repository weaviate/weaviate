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

package modstgazure

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

// Test user overrides
func TestUploadParams(t *testing.T) {
	defaultBlockSize := int64(40 * 1024 * 1024)
	defaultEnvironmentValue := int64(11)
	defaultHeaderValue := int64(13)
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	azure := New()
	os.Setenv("BACKUP_AZURE_CONTAINER", "test")
	os.Setenv("AZURE_STORAGE_ACCOUNT", "test")

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetLogger().Return(logrus.New())
	params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
	err := azure.Init(testCtx, params)
	require.Nil(t, err)

	t.Run("getBlockSize with no inputs", func(t *testing.T) {
		blockSize := azure.getBlockSize(testCtx)
		assert.Equal(t, defaultBlockSize, blockSize)
	})

	t.Run("getBlockSize with environment variable", func(t *testing.T) {
		t.Setenv("AZURE_BLOCK_SIZE", "11")
		azure := New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
		err := azure.Init(testCtx, params)
		assert.Nil(t, err)

		blockSize := azure.getBlockSize(testCtx)
		assert.Equal(t, defaultEnvironmentValue, blockSize)
	})

	t.Run("getBlockSize with invalid environment variable", func(t *testing.T) {
		t.Setenv("AZURE_BLOCK_SIZE", "invalid")
		azure := New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
		err := azure.Init(testCtx, params)
		assert.Nil(t, err)

		blockSize := azure.getBlockSize(testCtx)
		assert.Equal(t, defaultBlockSize, blockSize)
	})

	t.Run("getBlockSize with header", func(t *testing.T) {
		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Block-Size", []string{"13"})

		blockSize := azure.getBlockSize(ctxWithValue)
		assert.Equal(t, defaultHeaderValue, blockSize)
	})

	t.Run("getBlockSize with invalid header", func(t *testing.T) {
		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Block-Size", []string{"invalid"})

		blockSize := azure.getBlockSize(ctxWithValue)
		assert.Equal(t, defaultBlockSize, blockSize)
	})

	t.Run("getBlockSize with environment variable and header", func(t *testing.T) {
		t.Setenv("AZURE_BLOCK_SIZE", "11")
		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Block-Size", []string{"13"})

		blockSize := azure.getBlockSize(ctxWithValue)
		assert.Equal(t, defaultHeaderValue, blockSize)
	})

	t.Run("getConcurrency with no inputs", func(t *testing.T) {
		concurrency := azure.getConcurrency(testCtx)
		assert.Equal(t, 1, concurrency)
	})

	t.Run("getConcurrency with environment variable", func(t *testing.T) {
		t.Setenv("AZURE_CONCURRENCY", "11")
		azure := New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
		err := azure.Init(testCtx, params)
		assert.Nil(t, err)

		concurrency := azure.getConcurrency(testCtx)
		assert.Equal(t, defaultEnvironmentValue, int64(concurrency))
	})

	t.Run("getConcurrency with invalid environment variable", func(t *testing.T) {
		t.Setenv("AZURE_CONCURRENCY", "invalid")
		azure := New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
		err := azure.Init(testCtx, params)
		assert.Nil(t, err)

		concurrency := azure.getConcurrency(testCtx)
		assert.Equal(t, 1, concurrency)
	})

	t.Run("getConcurrency with header", func(t *testing.T) {
		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Concurrency", []string{"13"})

		concurrency := azure.getConcurrency(ctxWithValue)
		assert.Equal(t, defaultHeaderValue, int64(concurrency))
	})

	t.Run("getConcurrency with invalid header", func(t *testing.T) {
		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Concurrency", []string{"invalid"})

		concurrency := azure.getConcurrency(ctxWithValue)
		assert.Equal(t, 1, concurrency)
	})

	t.Run("getConcurrency with environment variable and header", func(t *testing.T) {
		t.Setenv("AZURE_CONCURRENCY", "11")
		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Concurrency", []string{"13"})

		concurrency := azure.getConcurrency(ctxWithValue)
		assert.Equal(t, defaultHeaderValue, int64(concurrency))
	})
}

func TestResolveContainer(t *testing.T) {
	tests := []struct {
		name            string
		configContainer string
		override        string
		wantContainer   string
		wantErr         string
	}{
		{
			name:            "uses config container when no override",
			configContainer: "my-container",
			override:        "",
			wantContainer:   "my-container",
		},
		{
			name:            "override replaces config container",
			configContainer: "my-container",
			override:        "other-container",
			wantContainer:   "other-container",
		},
		{
			name:            "empty config container without override returns error",
			configContainer: "",
			override:        "",
			wantErr:         "container must not be empty",
		},
		{
			name:            "empty config container with override succeeds",
			configContainer: "",
			override:        "override-container",
			wantContainer:   "override-container",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &azureClient{config: clientConfig{Container: tt.configContainer}}
			container, err := client.resolveContainer(tt.override)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantContainer, container)
			}
		})
	}
}

func TestAllBackupsSkipsMissingDescriptors(t *testing.T) {
	const containerName = "test-container"

	validDesc := []byte(`{"id":"backup-1"}`)

	tests := []struct {
		name             string
		downloadResponse func(blobName string) (status int, headers map[string]string, body []byte)
		wantIDs          []string
		wantErr          bool
	}{
		{
			name: "missing descriptor for one backup is skipped, other returned",
			downloadResponse: func(blobName string) (int, map[string]string, []byte) {
				switch blobName {
				case "backup-1/" + ubak.GlobalBackupFile:
					return http.StatusOK, nil, validDesc
				case "backup-2/" + ubak.GlobalBackupFile:
					return http.StatusNotFound,
						map[string]string{"x-ms-error-code": "BlobNotFound"},
						nil
				}
				return http.StatusNotFound,
					map[string]string{"x-ms-error-code": "BlobNotFound"}, nil
			},
			wantIDs: []string{"backup-1"},
		},
		{
			name: "non-not-found error on descriptor fetch fails the listing",
			downloadResponse: func(blobName string) (int, map[string]string, []byte) {
				if blobName == "backup-1/"+ubak.GlobalBackupFile {
					return http.StatusOK, nil, validDesc
				}
				return http.StatusInternalServerError,
					map[string]string{"x-ms-error-code": "InternalError"}, nil
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()

			// Hierarchy list: GET /<container>?restype=container&comp=list&prefix=&delimiter=/
			// Returns two BlobPrefix entries (one per backup ID).
			mux.HandleFunc("/"+containerName, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("comp") != "list" {
					// Treat as a blob download with empty name; reject.
					http.Error(w, "unexpected", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?>`+
					`<EnumerationResults ContainerName="`+containerName+`">`+
					`<Blobs>`+
					`<BlobPrefix><Name>backup-1/</Name></BlobPrefix>`+
					`<BlobPrefix><Name>backup-2/</Name></BlobPrefix>`+
					`</Blobs>`+
					`<NextMarker/>`+
					`</EnumerationResults>`)
			})

			// Blob download: GET /<container>/<blob path>
			mux.HandleFunc("/"+containerName+"/", func(w http.ResponseWriter, r *http.Request) {
				raw := strings.TrimPrefix(r.URL.Path, "/"+containerName+"/")
				name, err := url.PathUnescape(raw)
				if err != nil {
					http.Error(w, "bad blob name", http.StatusBadRequest)
					return
				}
				status, headers, body := tt.downloadResponse(name)
				for k, v := range headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(status)
				w.Write(body)
			})

			srv := httptest.NewServer(mux)
			defer srv.Close()

			// Disable retries so a 5xx surfaces immediately.
			client, err := azblob.NewClientWithNoCredential(srv.URL+"/", &azblob.ClientOptions{
				ClientOptions: policy.ClientOptions{
					Retry: policy.RetryOptions{MaxRetries: -1},
				},
			})
			require.NoError(t, err)

			a := &azureClient{
				client: client,
				config: clientConfig{Container: containerName},
				logger: logrus.New(),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			got, err := a.AllBackups(ctx)
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

type fakeStorageProvider struct {
	dataPath string
}

func (f *fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (f *fakeStorageProvider) DataPath() string {
	return f.dataPath
}
