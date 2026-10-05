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

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/wl/selfrecovery"
)

type testSchema struct{ err error }

func (s testSchema) ShardReplicas(class, shard string) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	return []string{"self"}, nil
}

type testPaths struct{ root string }

func (p testPaths) ShardPath(collection, shard string) string {
	return filepath.Join(p.root, collection, shard)
}

func TestSetupHandlers(t *testing.T) {
	for _, tc := range []struct {
		name         string
		path         string
		method       string
		query        string
		nilOrch      bool
		schemaErr    bool
		liveDir      bool
		status       int
		bodyContains string
	}{
		{name: "restart GET", path: RestartPath, method: http.MethodGet, query: "collection=C&shard=S", status: http.StatusMethodNotAllowed},
		{name: "restart bad collection", path: RestartPath, method: http.MethodPost, query: "collection=../x&shard=S", status: http.StatusBadRequest},
		{name: "restart bad shard", path: RestartPath, method: http.MethodPost, query: "collection=C&shard=a/b", status: http.StatusBadRequest},
		{name: "restart nil orchestrator", path: RestartPath, method: http.MethodPost, query: "collection=C&shard=S", nilOrch: true, status: http.StatusServiceUnavailable},
		{name: "restart unknown shard", path: RestartPath, method: http.MethodPost, query: "collection=C&shard=S", schemaErr: true, status: http.StatusNotFound},
		{name: "restart live shard", path: RestartPath, method: http.MethodPost, query: "collection=C&shard=S", liveDir: true, status: http.StatusConflict},
		{name: "restart recovering shard", path: RestartPath, method: http.MethodPost, query: "collection=C&shard=S", status: http.StatusAccepted, bodyContains: "restarted"},
		{name: "accept-empty GET", path: AcceptEmptyPath, method: http.MethodGet, query: "collection=C&shard=S", status: http.StatusMethodNotAllowed},
		{name: "accept-empty bad shard", path: AcceptEmptyPath, method: http.MethodPost, query: "collection=C&shard=..", status: http.StatusBadRequest},
		{name: "accept-empty nil orchestrator", path: AcceptEmptyPath, method: http.MethodPost, query: "collection=C&shard=S", nilOrch: true, status: http.StatusServiceUnavailable},
		{name: "accept-empty unknown shard", path: AcceptEmptyPath, method: http.MethodPost, query: "collection=C&shard=S", schemaErr: true, status: http.StatusNotFound},
		{name: "accept-empty known shard", path: AcceptEmptyPath, method: http.MethodPost, query: "collection=C&shard=S", status: http.StatusAccepted, bodyContains: "accepted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			root := t.TempDir()
			if tc.liveDir {
				require.NoError(t, os.MkdirAll(filepath.Join(root, "C", "S"), 0o755))
			}
			var schemaErr error
			if tc.schemaErr {
				schemaErr = errors.New("shard not found")
			}
			var orch *selfrecovery.Orchestrator
			if !tc.nilOrch {
				orch = selfrecovery.New(selfrecovery.Config{
					Schema:       testSchema{err: schemaErr},
					PathResolver: testPaths{root: root},
					NodeName:     "self",
					Logger:       logger,
				})
				t.Cleanup(func() { require.NoError(t, orch.Close(context.Background())) })
			}
			mux := http.NewServeMux()
			SetupHandlers(mux, logger, orch)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path+"?"+tc.query, nil))

			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.bodyContains)
		})
	}
}
