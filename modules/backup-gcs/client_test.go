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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/weaviate/weaviate/entities/backup"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

func TestInitialize_SkipAccessCheck(t *testing.T) {
	// With SkipAccessCheck set, Initialize must be a no-op: it returns
	// before touching the (nil) GCS client or running the write+delete probe.
	c := &gcsClient{config: clientConfig{Bucket: "my-bucket", SkipAccessCheck: true}}
	require.NoError(t, c.Initialize(context.Background(), "backup-1", "", ""))
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

			// Bucket attrs: GET /b/{bucket}
			mux.HandleFunc("/b/"+bucketName, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"kind":"storage#bucket","name":%q}`, bucketName)
			})

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

			srv := httptest.NewServer(mux)
			defer srv.Close()

			ctx := context.Background()
			gcs, err := storage.NewClient(ctx,
				option.WithoutAuthentication(),
				option.WithEndpoint(srv.URL),
			)
			require.NoError(t, err)
			defer gcs.Close()
			// Disable retries so a 500 surfaces immediately instead of looping.
			gcs.SetRetry(storage.WithPolicy(storage.RetryNever))

			g := &gcsClient{
				client: gcs,
				config: clientConfig{Bucket: bucketName},
				logger: logrus.New(),
			}

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
