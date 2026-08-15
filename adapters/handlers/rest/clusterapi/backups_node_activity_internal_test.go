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

package clusterapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

// A node too old to have a probe route answers the path from the cluster mux's
// catch-all, and the caller reads exactly those bytes as "too old to ask". The
// shape below is a released binary's, which is frozen: it stays unauthenticated
// however this build's routes are secured later.
func TestOldNodeAnswersTheProbePathFromTheCatchAll(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", index())
	server := httptest.NewServer(mux)
	defer server.Close()

	res, err := http.Get(server.URL + clusterprobe.BackupNodeActivityPath)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, res.StatusCode)
	assert.Equal(t, clusterprobe.NodeNotFoundHeaderValue,
		res.Header.Get(clusterprobe.NodeNotFoundHeader))
	assert.Equal(t, clusterprobe.NodeNotFoundBody, strings.TrimSpace(string(body)))
}

// A node that serves the route but has no probe wired must answer 503, not 404:
// 404 is the one code that tells the caller to give up and let this node pass.
// The realistic wiring failure is a nil *backup.NodeActivityProbe, which a plain
// interface nil check does not catch.
func TestBackupNodeActivityWithoutAProbe(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	backups := NewBackups(nil, nodeActivityProbe(nil), NewNoopAuthHandler(), logger)
	req := httptest.NewRequest(http.MethodGet, clusterprobe.BackupNodeActivityPath, nil)
	rec := httptest.NewRecorder()

	backups.NodeActivity().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
