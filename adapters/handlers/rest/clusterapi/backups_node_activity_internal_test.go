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
	"testing"

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
	assert.Equal(t, []string{clusterprobe.NodeNotFoundHeaderValue},
		res.Header.Values(clusterprobe.NodeNotFoundHeader),
		"the node writes the sentinel once, so a second value came from elsewhere")
	assert.Equal(t, clusterprobe.NodeNotFoundBody, string(body),
		"the constants are what the caller compares byte for byte, so they are net/http's own bytes")
}
