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

package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/wl/selfrecovery"
)

type selfRecoveryTestSchema struct{ err error }

func (s selfRecoveryTestSchema) ShardReplicas(class, shard string) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	return []string{"self"}, nil
}

type selfRecoveryTestPaths struct{ root string }

func (p selfRecoveryTestPaths) ShardPath(collection, shard string) string {
	return filepath.Join(p.root, collection, shard)
}

func TestSelfRecoveryHandlers(t *testing.T) {
	for _, tc := range []struct {
		name         string
		accept       bool
		method       string
		query        string
		nilOrch      bool
		licensed     bool
		schemaErr    bool
		status       int
		bodyContains string
	}{
		{name: "restart GET", method: http.MethodGet, query: "collection=C&shard=S", licensed: true, status: http.StatusMethodNotAllowed},
		{name: "restart bad collection", method: http.MethodPost, query: "collection=../x&shard=S", licensed: true, status: http.StatusBadRequest},
		{name: "restart nil orchestrator", method: http.MethodPost, query: "collection=C&shard=S", nilOrch: true, status: http.StatusServiceUnavailable},
		{name: "restart unlicensed unknown shard", method: http.MethodPost, query: "collection=C&shard=S", schemaErr: true, status: http.StatusNotFound},
		{name: "restart unlicensed known shard", method: http.MethodPost, query: "collection=C&shard=S", status: http.StatusForbidden, bodyContains: "license"},
		{name: "restart licensed unknown shard", method: http.MethodPost, query: "collection=C&shard=S", licensed: true, schemaErr: true, status: http.StatusNotFound},
		{name: "accept-empty unlicensed unknown shard", accept: true, method: http.MethodPost, query: "collection=C&shard=S", schemaErr: true, status: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			var schemaErr error
			if tc.schemaErr {
				schemaErr = errors.New("shard not found")
			}
			var orch *selfrecovery.Orchestrator
			if !tc.nilOrch {
				orch = selfrecovery.New(selfrecovery.Config{
					Enabled:      true,
					Licensed:     tc.licensed,
					Schema:       selfRecoveryTestSchema{err: schemaErr},
					PathResolver: selfRecoveryTestPaths{root: t.TempDir()},
					NodeName:     "self",
					Logger:       logger,
				})
				t.Cleanup(func() { require.NoError(t, orch.Close(context.Background())) })
			}
			handler, path := newSelfRecoveryRestartHandler(logger, orch), "/debug/self-recovery/restart"
			if tc.accept {
				handler, path = newSelfRecoveryAcceptEmptyHandler(logger, orch), "/debug/self-recovery/accept-empty"
			}

			rec := httptest.NewRecorder()
			handler(rec, httptest.NewRequest(tc.method, path+"?"+tc.query, nil))

			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.bodyContains)
		})
	}
}
